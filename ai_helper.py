"""
AI Helper - Intelligent FAQ matching and response generation using Gemini
FIXED VERSION - Actually works with current Gemini API
"""

import google.generativeai as genai
import json
from typing import List, Dict, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class AIHelper:
    """AI-powered chatbot intelligence using Google Gemini"""
    
    def __init__(self, api_key: str, model_name: str = 'models/gemini-1.5-flash'):
        """
        Initialize AI helper
        
        Args:
            api_key: Google Gemini API key
            model_name: Model to use (must include 'models/' prefix)
        """
        self.api_key = api_key
        self.model_name = model_name
        self.enabled = bool(api_key and api_key != '')
        
        if self.enabled:
            try:
                genai.configure(api_key=api_key)
                
                # Debug: List available models
                print("\n🔍 Checking available Gemini models...")
                available_models = []
                try:
                    for m in genai.list_models():
                        if 'generateContent' in m.supported_generation_methods:
                            available_models.append(m.name)
                            print(f"  ✓ {m.name}")
                except Exception as list_error:
                    print(f"  ⚠️ Could not list models: {list_error}")
                
                # Initialize model
                print(f"\n🚀 Initializing model: {model_name}")
                self.model = genai.GenerativeModel(model_name)
                
                # Test it works
                print("🧪 Testing model...")
                test_response = self.model.generate_content("Say 'OK'")
                print(f"✅ Gemini test successful: {test_response.text[:50]}")
                
                logger.info(f"✅ AI Helper initialized with {model_name}")
                
            except Exception as e:
                logger.error(f"❌ Failed to initialize Gemini: {e}")
                print(f"\n❌ Gemini initialization error: {e}")
                print(f"💡 Try these models instead:")
                print(f"   - models/gemini-1.5-flash-latest")
                print(f"   - models/gemini-1.5-pro")
                print(f"   - models/gemini-pro")
                self.enabled = False
        else:
            logger.warning("⚠️ AI Helper disabled - no API key provided")
            print("⚠️ AI Helper disabled - no API key provided")

    class AIHelper:
        def __init__(self, api_key: str, model_name: str = 'models/gemini-2.5-flash-preview-09-2025'):
        # ... existing init code ...
        
        # Add response cache
            self.response_cache = {}
            self.cache_ttl = 300  # 5 minutes
    
    def find_best_faq(self, user_message: str, faqs: List[Dict]) -> Tuple[Optional[Dict], float]:
        """
        Find the most relevant FAQ using AI understanding
        
        Args:
            user_message: User's question
            faqs: List of FAQ dictionaries
        
        Returns:
            Tuple of (best_faq, confidence_score)
        """
        if not self.enabled or not faqs:
            return None, 0.0
        
        try:
            # Create FAQ context for AI
            faq_context = self._format_faqs_for_ai(faqs)
            
            # Prompt for finding best match
            prompt = f"""You are a FAQ matching expert. Analyze the user's question and find the most relevant FAQ.

User Question: "{user_message}"

Available FAQs:
{faq_context}

Task:
1. Identify which FAQ best answers the user's question
2. Return ONLY a JSON object with this format:
{{"faq_id": "the_id_of_best_matching_faq", "confidence": 0.95, "reason": "brief explanation"}}

If no FAQ is relevant (confidence < 0.5), return:
{{"faq_id": null, "confidence": 0.0, "reason": "no relevant FAQ found"}}

Return ONLY the JSON, no other text."""

            # Generate response
            response = self.model.generate_content(prompt)
            result = self._parse_json_response(response.text)
            
            if result and result.get('faq_id'):
                # Find the FAQ
                faq = next((f for f in faqs if f.get('id') == result['faq_id']), None)
                confidence = float(result.get('confidence', 0.0))
                
                logger.info(f"AI matched FAQ: {result['faq_id']} (confidence: {confidence})")
                return faq, confidence
            
            return None, 0.0
            
        except Exception as e:
            logger.error(f"AI matching error: {e}")
            return None, 0.0
    
    def generate_smart_response(self, user_message: str, faq: Dict, context: Optional[List[Dict]] = None) -> str:
        """
        Generate a natural, context-aware response
        
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
            prompt = f"""You are a helpful, friendly customer support assistant. Generate a natural response to the user's question.

{"Recent Conversation:\n" + conversation_history + "\n" if conversation_history else ""}
User's Current Question: "{user_message}"

FAQ Information:
Question: {faq.get('question')}
Answer: {faq.get('answer')}

Task:
Generate a natural, conversational response that:
1. Answers the user's question using the FAQ information
2. Matches the tone and style of the FAQ answer
3. Feels personal and helpful
4. Is concise (2-3 sentences max)
5. Includes the emoji from the FAQ answer if present

Return ONLY the response text, no explanations or meta-commentary."""

            response = self.model.generate_content(prompt)
            generated_text = response.text.strip()
            
            # Fallback to original answer if generation fails
            if not generated_text or len(generated_text) < 10:
                return faq.get('answer', '')
            
            logger.info(f"AI generated response: {generated_text[:50]}...")
            return generated_text
            
        except Exception as e:
            logger.error(f"AI response generation error: {e}")
            return faq.get('answer', '')
    
    def understand_intent(self, user_message: str, lead_triggers: List[str]) -> Dict:
        """
        Understand user's intent (question, lead request, complaint, etc.)
        
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
            prompt = f"""Analyze the user's intent from this message: "{user_message}"

Possible intents:
- question: User asking for information
- lead_request: User wants to contact sales, get demo, pricing, or speak with human
- complaint: User expressing dissatisfaction
- greeting: User saying hi/hello
- gratitude: User saying thanks
- goodbye: User ending conversation

Lead trigger words: {', '.join(lead_triggers)}

Return ONLY JSON:
{{"intent": "intent_name", "confidence": 0.95, "action": "suggested_action"}}

Actions can be: answer, collect_lead, escalate, acknowledge"""

            response = self.model.generate_content(prompt)
            result = self._parse_json_response(response.text)
            
            if result:
                logger.info(f"Intent detected: {result.get('intent')} ({result.get('confidence')})")
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
            prompt = f"""Should this conversation be escalated to a human agent?

User message: "{user_message}"
Conversation length: {conversation_length} messages

Escalate if:
- User explicitly requests human contact
- User expresses strong frustration/anger
- Conversation is going in circles (length > 8)
- User has complex/custom requirements
- User mentions legal/compliance issues

Return ONLY JSON:
{{"should_escalate": true, "reason": "brief explanation"}}

or

{{"should_escalate": false, "reason": "brief explanation"}}"""

            response = self.model.generate_content(prompt)
            result = self._parse_json_response(response.text)
            
            if result:
                should_escalate = result.get('should_escalate', False)
                logger.info(f"Escalation check: {should_escalate} - {result.get('reason')}")
                return should_escalate
            
            return False
            
        except Exception as e:
            logger.error(f"Escalation check error: {e}")
            return False
    
    def _format_faqs_for_ai(self, faqs: List[Dict]) -> str:
        """Format FAQs for AI context"""
        formatted = []
        for faq in faqs[:20]:  # Limit to 20 FAQs to avoid token limits
            formatted.append(f"ID: {faq.get('id')}\nQ: {faq.get('question')}\nA: {faq.get('answer')[:100]}...")
        return "\n\n".join(formatted)
    
    def _parse_json_response(self, text: str) -> Optional[Dict]:
        """Parse JSON from AI response, handling markdown code blocks"""
        try:
            # Remove markdown code blocks if present
            text = text.strip()
            if text.startswith('```'):
                # Remove ```json and ``` markers
                lines = text.split('\n')
                text = '\n'.join(lines[1:-1]) if len(lines) > 2 else text
                if text.startswith('json'):
                    text = text[4:]
            
            text = text.strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"JSON parsing error: {e}\nText: {text}")
            return None


# Singleton instance
_ai_helper = None


def get_ai_helper(api_key: str, model_name: str = 'models/gemini-1.5-flash'):
    """Get or create AI helper singleton"""
    global _ai_helper
    if _ai_helper is None:
        _ai_helper = AIHelper(api_key, model_name)
    return _ai_helper