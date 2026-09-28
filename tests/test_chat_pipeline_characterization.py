"""Characterize current chat routing and response contracts without network IO."""

import os
from unittest.mock import Mock, patch

os.environ.setdefault('DATABASE_URL', 'postgresql://unused:unused@localhost:5432/unused')

from flask import Flask

from blueprints import chat as chat_blueprint
from models import conversations

import utils as _utils

if not hasattr(_utils, 'log_crash'):
    _utils.log_crash = lambda *args, **kwargs: None
if not hasattr(_utils, 'generate'):
    _utils.generate = lambda *args, **kwargs: None

from pipeline.stages.intent import detect_intent, dispatch_tool


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = None
        self.params = None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        if self.params and len(self.params) == 3:
            session_id = self.params[1]
            return [row for row in self.rows if row.get('session_id') == session_id]
        return self.rows

    def close(self):
        pass


class _FakeConnection:
    def close(self):
        pass


class TestIntentRoutingCharacterization:
    def test_simple_intent_precedes_action_and_tool_keywords(self):
        intent = detect_intent(
            'thanks, book a demo, where is my order',
            vertical='general', lead_triggers=[], model=None, skip_gemini=True,
        )
        assert intent['intent'] == 'gratitude'
        assert intent['action'] is None
        assert intent['tool'] is None

    def test_action_precedes_tool_keyword(self):
        intent = detect_intent(
            'book a demo and where is my order',
            vertical='general', lead_triggers=[], model=None, skip_gemini=True,
        )
        assert intent['intent'] == 'action'
        assert intent['action'] == 'demo_request'
        assert intent['tool'] is None

    def test_longest_tool_keyword_wins(self):
        intent = detect_intent(
            'cancel my order',
            vertical='general', lead_triggers=[], model=None, skip_gemini=True,
        )
        assert intent['intent'] == 'tool'
        assert intent['tool'] == 'cancel_order'

    def test_product_keyword_dispatches_to_search_products(self):
        message = 'do you sell blue widget'
        intent = detect_intent(
            message,
            vertical='general', lead_triggers=[], model=None, skip_gemini=True,
        )
        tool_result = {
            'success': True,
            'products': [{'title': 'Blue Widget', 'available': True}],
            'count': 1,
            'query': message,
        }
        assert intent['intent'] == 'tool'
        assert intent['tool'] == 'search_products'

        with patch('tools.search_products', return_value=tool_result) as search:
            response = dispatch_tool(intent['tool'], message, 'tenant-x', {})

        search.assert_called_once_with(client_id='tenant-x', query=message)
        assert response['success'] is True
        assert response['data'] == tool_result
        assert 'Blue Widget' in response['response']


class TestChatResponseCharacterization:
    def test_api_chat_normal_ai_response_json_shape(self):
        class FakeAIHelper:
            enabled = True

            def generate_response(self, **kwargs):
                self.kwargs = kwargs
                return {
                    'response': 'The store is open today.',
                    'method': 'rag',
                    'confidence': 0.82,
                    'is_lead': False,
                    'action': None,
                }

        helper = FakeAIHelper()
        app = Flask(__name__)
        app.register_blueprint(chat_blueprint.chat_bp)
        chat_blueprint.init_chat(
            limiter=None,
            ai_helper=helper,
            plan_limits={},
            vertical_prompts={},
            log_conversation=Mock(),
            find_best_match=Mock(return_value=(None, 0.0)),
            get_cached_client_owner=Mock(return_value=None),
            fire_webhook=Mock(),
            notify_handoff=Mock(),
            notify_usage_threshold=Mock(),
        )

        with (
            patch.object(chat_blueprint.models, 'get_client_by_id', return_value=None),
            patch.object(chat_blueprint.models, 'get_recent_conversations', return_value=[]) as recent_history,
        ):
            response = app.test_client().post('/api/chat', json={
                'message': 'Are you open?',
                'client_id': 'demo',
                'session_id': 'session-demo',
                'history': [],
            })

        assert response.status_code == 200
        assert response.get_json() == {
            'success': True,
            'response': 'The store is open today.',
            'confidence': 0.82,
            'method': 'rag',
            'session_id': 'session-demo',
        }
        assert helper.kwargs['conversation_history'] == []
        recent_history.assert_called_once_with('demo', 'session-demo', limit=15)


class TestConversationHistoryCharacterization:
    def test_recent_history_isolated_by_session_for_same_client(self):
        rows = [
            {
                'session_id': 'session-b',
                'user_message': 'Private question from B',
                'bot_response': 'Private answer for B',
            },
            {
                'session_id': 'session-a',
                'user_message': 'Private question from A',
                'bot_response': 'Private answer for A',
            },
        ]
        cursor = _FakeCursor(rows)
        with patch.object(conversations, 'get_db', return_value=(_FakeConnection(), cursor)):
            history = conversations.get_recent_conversations(
                'tenant-x', 'session-a', limit=15,
            )

        assert cursor.params == ('tenant-x', 'session-a', 15)
        assert 'session_id = %s' in cursor.sql
        assert history == [
            {'role': 'user', 'content': 'Private question from A'},
            {'role': 'assistant', 'content': 'Private answer for A'},
        ]
