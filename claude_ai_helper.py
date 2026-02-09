"""
AI Helper - Using Claude API (Anthropic)
Much better than Gemini - more accurate, reliable, and follows instructions perfectly!
"""

from anthropic import Anthropic
import json
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class ClaudeAIHelper:
    """AI-powered chatbot intelligence using Claude (Anthropic)"""
    
    def __init__(self, api_key: str, model_name: str = 'claude-3-5-haiku-20241022'):
        """
        Initialize Claude AI helper
        
        Args:
            api_key: Anthropic API key
            model_name: Model to use (haiku = fast/cheap, sonnet = powerful)
        """
        self.api_key = api_key
        self.model_name = model_name
        self.enabled = bool(api_key and api_key.startswith('sk-ant-'))
        
        if self.enabled:
            try:
                self.client = Anthropic(api_key=api_key)
                logger.info(f"✅ Claude AI Helper initialized with {model_name}")
            except Exception as e:
                logger.error(f"Failed to initialize Claude: {e}")
                self.enabled = False
        else:
            logger.warning("⚠️ Claude AI Helper disabled - no API key provided")
    
    def find_best_faq(self, user_message: str, faqs: List[Dict]) -> Tuple[Optional[Dict], float]:
        """
        Find the most relevant FAQ using Claude's understanding
        
        Args:
            user_message: User's question
            faqs: List of FAQ dictionaries
        
        Returns:
            Tuple of (best_faq, confidence_score)
        """
        if not self.enabled or not faqs:
            return None, 0.0
        
        try:
            # Create FAQ context for Claude
            faq_context = self._format_faqs_for_claude(faqs)
            
            # Prompt for finding best match
            message = self.client.messages.create(
                model=self.model_name,
                max_tokens=500,
                messages=[{
                    "role": "user",
                    "content": f"""You are a FAQ matching expert. Find the most relevant FAQ for this user question.

User Question: "{user_message}"

Available FAQs:
{faq_context}

Return ONLY a JSON object (no markdown, no explanation):
{{"faq_id": "the_id_of_best_match", "confidence": 0.95, "reason": "brief explanation"}}

If no FAQ is relevant (confidence < 0.5):
{{"faq_id": null, "confidence": 0.0, "reason": "no relevant FAQ"}}"""
                }]
            )
            
            response_text = message.content[0].text
            result = self._parse_json_response(response_text)
            
            if result and result.get('faq_id'):
                # Find the FAQ
                faq = next((f for f in faqs if f.get('id') == result['faq_id']), None)
                confidence = float(result.get('confidence', 0.0))
                
                logger.info(f"Claude matched FAQ: {result['faq_id']} (confidence: {confidence})")
                return faq, confidence
            
            return None, 0.0
            
        except Exception as e:
            logger.error(f"Claude matching error: {e}")
            return None, 0.0
    
    def generate_smart_response(self, user_message: str, faq: Dict, context: Optional[List[Dict]] = None) -> str:
        """
        Generate a natural, context-aware response using Claude
        
        Args:
            user_message: User's question
            faq: Matched FAQ
            context: Previous conversation messages
        
        Returns:
            Natural response string
        """
        if not self.enabled:
            return faq.get('answer', '')
        
        try:
            # Build conversation context
            conversation_history = ""
            if context:
                conversation_history = "\n".join([
                    f"{'User' if msg.get('role') == 'user' else 'Assistant'}: {msg.get('content')}"
                    for msg in context[-3:]  # Last 3 messages
                ])
            
            # Prompt for natural response
            system_prompt = "You are a helpful, friendly customer support assistant. Generate natural, conversational responses."
            
            user_prompt = f"""Generate a natural response to the user's question using this FAQ information.

{"Recent Conversation:\n" + conversation_history + "\n" if conversation_history else ""}
User's Question: "{user_message}"

FAQ Information:
Question: {faq.get('question')}
Answer: {faq.get('answer')}

Requirements:
- Answer naturally and conversationally
- Keep the tone and style of the FAQ answer
- Be concise (2-3 sentences max)
- Include any emojis from the FAQ answer
- Make it feel personal and helpful

Return ONLY the response text, no meta-commentary."""

            message = self.client.messages.create(
                model=self.model_name,
                max_tokens=300,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}]
            )
            
            generated_text = message.content[0].text.strip()
            
            # Fallback to original answer if generation seems weird
            if not generated_text or len(generated_text) < 10:
                return faq.get('answer', '')
            
            logger.info(f"Claude generated response: {generated_text[:50]}...")
            return generated_text
            
        except Exception as e:
            logger.error(f"Claude response generation error: {e}")
            return faq.get('answer', '')
    
    def understand_intent(self, user_message: str, lead_triggers: List[str]) -> Dict:
        """
        Understand user's intent using Claude
        
        Args:
            user_message: User's message
            lead_triggers: Words that trigger lead collection
        
        Returns:
            Dict with intent, confidence, and action
        """
        if not self.enabled:
            # Fallback to simple keyword matching
            message_lower = user_message.lower()
            for trigger in lead_triggers:
                if trigger.lower() in message_lower:
                    return {
                        'intent': 'lead_request',
                        'confidence': 0.8,
                        'action': 'collect_lead'
                    }
            return {'intent': 'question', 'confidence': 0.5, 'action': 'answer'}
        
        try:
            message = self.client.messages.create(
                model=self.model_name,
                max_tokens=200,
                messages=[{
                    "role": "user",
                    "content": f"""Analyze the user's intent: "{user_message}"

Possible intents:
- question: Asking for information
- lead_request: Wants to contact sales/demo/pricing/human
- complaint: Expressing dissatisfaction
- greeting: Saying hi/hello
- gratitude: Saying thanks
- goodbye: Ending conversation

Lead trigger words: {', '.join(lead_triggers)}

Return ONLY JSON:
{{"intent": "intent_name", "confidence": 0.95, "action": "suggested_action"}}

Actions: answer, collect_lead, escalate, acknowledge"""
                }]
            )
            
            response_text = message.content[0].text
            result = self._parse_json_response(response_text)
            
            if result:
                logger.info(f"Claude detected intent: {result.get('intent')} ({result.get('confidence')})")
                return result
            
            return {'intent': 'question', 'confidence': 0.5, 'action': 'answer'}
            
        except Exception as e:
            logger.error(f"Intent understanding error: {e}")
            return {'intent': 'question', 'confidence': 0.5, 'action': 'answer'}
    
    def should_escalate(self, user_message: str, conversation_length: int) -> bool:
        """
        Determine if conversation should be escalated to human
        
        Args:
            user_message: Current user message
            conversation_length: Number of messages in conversation
        
        Returns:
            True if should escalate
        """
        if not self.enabled:
            # Simple heuristic fallback
            escalation_words = ['manager', 'supervisor', 'complaint', 'angry', 'frustrated']
            message_lower = user_message.lower()
            return any(word in message_lower for word in escalation_words) or conversation_length > 10
        
        try:
            message = self.client.messages.create(
                model=self.model_name,
                max_tokens=150,
                messages=[{
                    "role": "user",
                    "content": f"""Should this conversation be escalated to a human?

User: "{user_message}"
Conversation length: {conversation_length} messages

Escalate if:
- User explicitly requests human
- Strong frustration/anger
- Going in circles (length > 8)
- Complex/custom requirements
- Legal/compliance mentioned

Return ONLY JSON:
{{"should_escalate": true/false, "reason": "brief explanation"}}"""
                }]
            )
            
            response_text = message.content[0].text
            result = self._parse_json_response(response_text)
            
            if result:
                should_escalate = result.get('should_escalate', False)
                logger.info(f"Escalation check: {should_escalate} - {result.get('reason')}")
                return should_escalate
            
            return False
            
        except Exception as e:
            logger.error(f"Escalation check error: {e}")
            return False
    
    def _format_faqs_for_claude(self, faqs: List[Dict]) -> str:
        """Format FAQs for Claude context"""
        formatted = []
        for faq in faqs[:20]:  # Limit to 20 FAQs
            formatted.append(f"ID: {faq.get('id')}\nQ: {faq.get('question')}\nA: {faq.get('answer')[:100]}...")
        return "\n\n".join(formatted)
    
    def _parse_json_response(self, text: str) -> Optional[Dict]:
        """Parse JSON from Claude response"""
        try:
            # Remove markdown code blocks if present
            text = text.strip()
            if text.startswith('```'):
                lines = text.split('\n')
                text = '\n'.join(lines[1:-1])  # Remove first and last lines
                if text.startswith('json'):
                    text = text[4:]
            
            text = text.strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"JSON parsing error: {e}\nText: {text}")
            return None


# Singleton instance
_claude_helper = None


def get_claude_helper(api_key: str, model_name: str = 'claude-3-5-haiku-20241022') -> ClaudeAIHelper:
    """Get or create Claude helper singleton"""
    global _claude_helper
    if _claude_helper is None:
        _claude_helper = ClaudeAIHelper(api_key, model_name)
    return _claude_helper