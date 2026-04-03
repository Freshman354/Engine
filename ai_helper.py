"""
AI Helper - Intelligent FAQ matching and response generation using Gemini.
Enhanced for natural, human-like conversations with 15-message memory.
"""

import google.generativeai as genai
import json
from typing import List, Dict, Tuple, Optional
import logging
import re

logger = logging.getLogger(__name__)

# ── Simple intent keywords — checked before AI to save tokens ────────
_SIMPLE_INTENTS = {
    'greeting':  ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening', 'howdy'],
    'gratitude': ['thanks', 'thank you', 'cheers', 'appreciate it', 'much appreciated', 'thx'],
    'goodbye':   ['bye', 'goodbye', 'see you', 'take care', 'cya', 'see ya', 'later'],
}


class AIHelper:
    """AI-powered chatbot with human-like responses and extended conversation memory."""

    def __init__(self, api_key: str, model_name: str = 'gemini-2.0-flash'):
        self.api_key   = api_key
        self.model_name = model_name
        self.enabled   = bool(api_key and api_key.strip())

        # Personality tones per vertical
        self.personalities = {
            'general':     "warm, friendly, and helpful — like a knowledgeable colleague",
            'real_estate': "enthusiastic, reassuring, and professional — make buying/renting feel exciting",
            'saas':        "patient, clear, and solution-oriented — great at explaining features",
            'ecommerce':   "fast, friendly, and shopper-focused — keep it quick and helpful",
            'healthcare':  "calm, empathetic, and professional — never give medical advice",
            'law_firm':    "formal, precise, trustworthy, and cautious — excellent at intake",
        }

        if self.enabled:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel(model_name)
                # No startup test call — saves tokens and startup latency
                logger.info(f"✅ AI Helper initialized with {model_name} — 15-message memory enabled")
            except Exception as e:
                logger.error(f"Failed to initialize Gemini: {e}")
                self.enabled = False
        else:
            logger.warning("AI Helper disabled — no GEMINI_API_KEY provided")

    # ── PUBLIC API ────────────────────────────────────────────────────

    def find_best_faq(self, user_message: str, faqs: List[Dict]) -> Tuple[Optional[Dict], float]:
        """Find the best matching FAQ using Gemini semantic understanding."""
        if not self.enabled or not faqs:
            return None, 0.0

        try:
            faq_context = self._format_faqs_for_ai(faqs)

            prompt = f"""You are an expert at matching user questions to FAQs.

User question: "{user_message}"

Available FAQs:
{faq_context}

Return ONLY valid JSON in this exact format:
{{
  "faq_id": "exact_id_of_best_match_or_null",
  "confidence": 0.92,
  "reason": "short explanation"
}}

If no FAQ is relevant enough (confidence < 0.45), set faq_id to null and confidence to 0.0."""

            response = self.model.generate_content(prompt)
            result   = self._parse_json_response(response.text)

            if result and result.get('faq_id'):
                faq        = next((f for f in faqs if str(f.get('id')) == str(result['faq_id'])), None)
                confidence = float(result.get('confidence', 0.0))
                if faq and confidence > 0.45:
                    logger.info(f"AI matched FAQ {result['faq_id']} | confidence: {confidence:.2f}")
                    return faq, confidence

            return None, 0.0

        except Exception as e:
            logger.error(f"find_best_faq error: {e}")
            return None, 0.0

    def generate_human_like_response(self, user_message: str, faq: Dict,
                                     vertical: str = 'general',
                                     conversation_history: List[Dict] = None) -> str:
        """
        Generate a natural, human-like response grounded in the matched FAQ.
        Uses up to 15 messages of conversation history for context.
        """
        if not self.enabled or not faq:
            return faq.get('answer', "I'm not sure about that. Would you like me to connect you with the team?") if faq else \
                   "I'm not sure about that. Would you like me to connect you with the team?"

        try:
            personality = self.personalities.get(vertical, "warm and helpful")
            history_str = self._format_conversation_history(conversation_history)

            prompt = f"""You are a {personality} customer support assistant.

Recent conversation (last 15 messages for context):
{history_str}

User just asked: "{user_message}"

Relevant FAQ knowledge:
Q: {faq.get('question')}
A: {faq.get('answer')}

Guidelines:
- Speak naturally like a helpful human (use contractions: I'm, you're, it's, don't)
- Be warm, concise, and engaging — 1 to 3 sentences maximum
- Use the FAQ as your source of truth but rephrase it conversationally
- Add a gentle follow-up question when it feels natural
- Use emojis sparingly and only when they genuinely fit the tone

Return ONLY the response text. No markdown, no quotes, no preamble."""

            response = self.model.generate_content(prompt)
            text     = response.text.strip()

            if not text or len(text) < 15:
                return self._make_conversational_fallback(faq.get('answer', ''))

            return text

        except Exception as e:
            logger.error(f"generate_human_like_response error: {e}")
            return self._make_conversational_fallback(faq.get('answer', ''))

    def generate_vertical_fallback(self, user_message: str, faqs: List[Dict],
                                   vertical: str = 'general') -> str:
        """
        Helpful fallback when no strong FAQ match exists.
        Keeps context by scanning the top FAQs and responding honestly.
        """
        if not self.enabled:
            return "I'm not entirely sure about that. Would you like me to connect you with the team?"

        try:
            personality = self.personalities.get(vertical, "warm and helpful")
            faq_context = self._format_faqs_for_ai(faqs[:12])

            prompt = f"""You are a {personality} assistant.

User asked: "{user_message}"

Knowledge base context:
{faq_context}

The question doesn't perfectly match any FAQ. Give a helpful, honest, and natural 2-sentence response.
If you can't answer accurately, politely offer to connect them with the team.
Sound friendly and human — not robotic.

Return ONLY the response text."""

            response = self.model.generate_content(prompt)
            text     = response.text.strip()
            return text if len(text) > 10 else \
                   "I'm happy to help! Could you tell me a bit more about what you're looking for?"

        except Exception as e:
            logger.error(f"generate_vertical_fallback error: {e}")
            return "I'm not sure I have the exact answer for that. Would you like me to connect you with the team?"

    def understand_intent(self, user_message: str, lead_triggers: List[str]) -> Dict:
        """
        Detect user intent. Uses keyword matching first (free, fast),
        falls back to Gemini only for ambiguous lead detection.
        """
        lower = user_message.lower().strip()

        # Fast keyword check for simple intents — no API call needed
        for intent, keywords in _SIMPLE_INTENTS.items():
            if any(lower == k or lower.startswith(k) for k in keywords):
                return {'intent': intent, 'confidence': 0.95}

        # Keyword check for configured lead triggers
        for trigger in lead_triggers:
            if trigger.lower() in lower:
                return {'intent': 'lead_request', 'confidence': 0.85}

        # Only call Gemini if the message is ambiguous and might be a lead request
        if self.enabled and len(user_message) > 8:
            try:
                prompt = f"""Classify this user message: "{user_message}"

Lead triggers to watch for: {', '.join(lead_triggers)}

Return ONLY JSON:
{{"intent": "question|lead_request|greeting|gratitude|complaint|goodbye", "confidence": 0.9}}"""

                response = self.model.generate_content(prompt)
                result   = self._parse_json_response(response.text)
                if result:
                    return result
            except Exception:
                pass

        return {'intent': 'question', 'confidence': 0.6}

    # ── PRIVATE HELPERS ───────────────────────────────────────────────

    def _format_conversation_history(self, history: List[Dict]) -> str:
        """Format last 15 messages for context."""
        if not history:
            return "(No previous messages)"

        recent    = history[-15:]
        formatted = []
        for msg in recent:
            role    = "User" if msg.get('role') == 'user' else "Assistant"
            content = msg.get('content', '').strip()
            if content:
                formatted.append(f"{role}: {content}")

        return "\n".join(formatted) if formatted else "(No previous messages)"

    def _format_faqs_for_ai(self, faqs: List[Dict]) -> str:
        """Format FAQs cleanly for AI context, truncating long answers safely."""
        formatted = []
        for faq in faqs:
            answer = faq.get('answer', '')
            # Only truncate if actually over 200 chars
            truncated = (answer[:200] + '…') if len(answer) > 200 else answer
            formatted.append(f"ID: {faq.get('id')}\nQ: {faq.get('question')}\nA: {truncated}")
        return "\n\n".join(formatted)

    def _make_conversational_fallback(self, answer: str) -> str:
        """Lightly humanise a raw FAQ answer when AI generation fails."""
        if not answer:
            return "I'm not sure about that one. Would you like me to connect you with the team?"

        text = (answer
                .replace(" I am ",   " I'm ")
                .replace(" You are ", " You're ")
                .replace(" it is ",  " it's ")
                .replace(" do not ", " don't ")
                .replace(" cannot ", " can't "))
        return text

    def _parse_json_response(self, text: str) -> Optional[Dict]:
        """Robust JSON parser — handles markdown code fences and stray text."""
        text = text.strip()
        if text.startswith('```'):
            text = re.sub(r'^```(?:json)?\s*|\s*```$', '', text, flags=re.DOTALL).strip()

        try:
            return json.loads(text)
        except Exception:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except Exception:
                    pass
        return None


# ── Singleton ─────────────────────────────────────────────────────────

_ai_helper: Optional['AIHelper'] = None


def get_ai_helper(api_key: str, model_name: str = 'gemini-2.0-flash') -> AIHelper:
    """Get or create the AI helper singleton."""
    global _ai_helper
    if _ai_helper is None:
        _ai_helper = AIHelper(api_key, model_name)
    return _ai_helper