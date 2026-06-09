"""
constants.py
============
All shared keyword lists, signals, regex patterns, and configuration
constants previously scattered as module-level variables in ai_helper.py.

Import from here wherever you need them. Adding a new keyword or adjusting
a threshold now has one place to look, not six.
"""

import re
from typing import Dict, List

# ── Simple intent keywords (Tier 1 — zero cost) ───────────────────────────────
SIMPLE_INTENTS: Dict[str, List[str]] = {
    'greeting':  ['hi', 'hello', 'hey', 'good morning', 'good afternoon', 'good evening'],
    'gratitude': ['thanks', 'thank you', 'cheers', 'appreciate', 'thx'],
    'goodbye':   ['bye', 'goodbye', 'see you', 'take care', 'cya'],
}

# ── Pricing / sales keywords ──────────────────────────────────────────────────
GLOBAL_PRICING_KW: List[str] = [
    'price', 'pricing', 'cost', 'how much', 'enterprise', 'plan',
    'subscription', 'buy', 'quote', 'invoice', 'billing',
]

# ── Action engine keyword maps ────────────────────────────────────────────────
ACTION_KEYWORDS: Dict[str, List[str]] = {
    'pricing_request': [
        'send pricing', 'pricing pdf', 'email me pricing',
        'email the pricing', 'send me pricing', 'pricing details',
        'get pricing', 'share pricing',
    ],
    'demo_request': [
        'book a demo', 'schedule a demo', 'request a demo',
        'arrange a demo', 'try it', 'see a demo',
        'want a demo', 'demo please', 'show me a demo',
    ],
    'meeting_request': [
        'schedule a call', 'book a call', 'set up a call',
        'arrange a call', 'have a meeting', 'book a meeting',
        'schedule a meeting', 'call me', 'schedule a time',
    ],
    'contact_request': [
        'contact sales', 'talk to sales', 'speak to sales',
        'reach sales', 'contact someone', 'speak to a person',
        'talk to a human', 'human agent', 'real person',
    ],
}

ACTION_LABELS: Dict[str, str] = {
    'demo_request':    'demo',
    'meeting_request': 'call',
    'pricing_request': 'pricing details',
    'contact_request': 'conversation with our team',
}

# ── Tool keywords (Phase 5 — real API dispatch) ───────────────────────────────
TOOL_KEYWORDS: Dict[str, List[str]] = {
    'lookup_order': [
        'where is my order', 'track my order', 'order status',
        "where's my package", 'wheres my package', 'track my package',
        'shipment status', 'delivery status', 'my order',
    ],
    'cancel_order': [
        'cancel my order', 'cancel order', 'i want to cancel',
        'stop my order', 'cancel this order',
    ],
    'check_availability': [
        'check availability', 'available slots', 'available times',
        'when are you available', 'what slots', 'free slots',
        'open appointments', 'available appointments',
    ],
    'book_appointment': [
        'book an appointment', 'schedule an appointment', 'book a slot',
        'book a time', 'set up an appointment', 'make an appointment',
        'reserve a time', 'i want to book',
    ],
    'escalate_to_human': [
        'speak to a human', 'talk to a person', 'human agent',
        'real person', 'talk to someone', 'connect me to support',
        'i need help from a person',
    ],
    'search_knowledge_base': [
        'search your docs', 'search your knowledge', 'look it up',
        'find in your docs', 'search for information',
    ],
}

# ── Purchase stage signals ────────────────────────────────────────────────────
STAGE_SIGNALS: Dict[str, List[str]] = {
    'browsing':   ['just looking', 'exploring', 'checking out', 'curious',
                   'what do you offer', 'tell me about'],
    'evaluating': ['compare', ' vs ', 'difference between', 'better than',
                   'pros and cons', 'which plan', 'which is best'],
    'buying':     ['sign up', 'sign me up', 'purchase', 'get started', 'upgrade',
                   'how do i pay', 'payment', 'checkout', 'subscribe'],
    'onboarding': ['how do i set up', 'getting started', 'first time',
                   'connect', 'install', 'embed', 'integrate'],
    'support':    ['not working', 'broken', 'error', 'issue', 'problem',
                   'help me fix', 'cant access', "doesn't work", 'bug'],
}

# Ordered list used as a one-way ratchet — stage can only advance, never regress.
STAGE_ORDER: List[str] = ['browsing', 'evaluating', 'buying', 'onboarding', 'support']

# ── Frustration and urgency signals ──────────────────────────────────────────
FRUSTRATION_SIGNALS: List[str] = [
    "that's wrong", "that's not right", "that doesn't help",
    "useless", "terrible", "awful", "hate this", "worst",
    "you're not helping", "not what i asked", "i already said",
    "i told you", "how many times", "are you serious", "this is ridiculous",
    "not helpful", "still doesn't work", "same problem", "again",
    "forget it", "never mind", "this is stupid",
]

BILLING_URGENCY_SIGNALS: List[str] = [
    'overcharged', 'charged twice', 'wrong charge', 'refund',
    'cancel my subscription', 'unauthorised charge', 'unauthorized charge',
    'dispute', 'charge my card', 'billing error', 'charged the wrong amount',
]

# ── IDK / non-KB pipeline methods ─────────────────────────────────────────────
# Used in session memory frustration logic and context building.
IDK_METHODS_ALL = frozenset({
    'idk_fallback', 'vertical_fallback', 'vertical_fallback_idk',
    'static_fallback', 'fatal_fallback', 'confidence_gate_handoff',
    'idk_no_kb', 'declined_handoff', 'escalation',
    'dynamic_fallback_idk',   # replaces the vertical_fallback → dynamic_fallback chain
})

# ── Multi-intent decomposition ────────────────────────────────────────────────
INTENT_SPLITTERS = re.compile(
    r'\b(?:and also|as well as|additionally|another question|'
    r'also (?:what|how|when|where|why|who|is|are|do|does|can)|'
    r'but (?:also|what|how)|'
    r'on top of that|while (?:i have you|we(?:\'re| are) at it))\b',
    re.IGNORECASE,
)
QUESTION_SIGNALS = re.compile(
    r'\b(?:what|how|when|where|why|who|is|are|do|does|can|will|would|could)\b',
    re.IGNORECASE,
)

# ── Bare pronoun set ──────────────────────────────────────────────────────────
BARE_PRONOUNS = frozenset({'it', 'that', 'this', 'they', 'them', 'those', 'these'})

# ── Stopwords for topic overlap ───────────────────────────────────────────────
OVERLAP_STOPWORDS = frozenset([
    'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can',
    'her', 'was', 'one', 'our', 'out', 'day', 'get', 'has', 'him',
    'his', 'how', 'its', 'let', 'may', 'now', 'old', 'see', 'two',
    'way', 'who', 'did', 'ask', 'use', 'via', 'per', 'than', 'then',
    'they', 'this', 'that', 'with', 'have', 'from', 'will', 'your',
    'what', 'when', 'where', 'which', 'there', 'been', 'does', 'more',
    'also', 'into', 'some', 'than', 'very', 'just', 'about', 'like',
])

# ── Retrieval config ──────────────────────────────────────────────────────────
MAX_CANDIDATES: int = 50

# ── Query expansion (inference time) ─────────────────────────────────────────
# Set True to re-enable (adds 1 unbudgeted Gemini call per turn on 3+ word queries).
INFERENCE_EXPANSION_ENABLED: bool = False

# ── Vertical personalities ────────────────────────────────────────────────────
# Moved from AIHelper.__init__ so generation stage can access them without
# holding a reference to the full AIHelper instance.
PERSONALITIES: Dict[str, Dict] = {
    'general': {
        'tone':             "warm, friendly, and helpful — like a knowledgeable colleague",
        'polish_hint':      "Keep it conversational and approachable. Use plain English.",
        'lead_keywords':    ['demo', 'speak to someone', 'contact me', 'call me',
                             'book a call', 'human', 'agent', 'talk to sales'],
        'pricing_keywords': GLOBAL_PRICING_KW,
    },
    'real_estate': {
        'tone':             "enthusiastic, reassuring, and professional",
        'polish_hint':      "Use upbeat, encouraging language. Mention next steps naturally.",
        'lead_keywords':    ['viewing', 'appointment', 'book a tour', 'schedule',
                             'visit the property', 'speak to agent'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['rent', 'mortgage', 'deposit'],
    },
    'saas': {
        'tone':             "patient, clear, and solution-oriented",
        'polish_hint':      "Be technically precise. Use bullet points when listing 3+ features.",
        'lead_keywords':    ['demo', 'trial', 'onboarding', 'integration',
                             'speak to sales', 'account manager'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['monthly fee', 'annual plan', 'seats'],
    },
    'ecommerce': {
        'tone':             "fast, friendly, and shopper-focused",
        'polish_hint':      "Short sentences. Get to the point fast. Always end with a CTA if relevant.",
        'lead_keywords':    ['order status', 'return', 'refund', 'speak to support'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['shipping cost', 'discount', 'promo'],
    },
    'healthcare': {
        'tone':             "calm, empathetic, and professional — never give medical advice",
        'polish_hint':      "Warm but careful tone. Never diagnose. Direct to professionals when needed.",
        'lead_keywords':    ['appointment', 'booking', 'schedule', 'consultation', 'see a doctor'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['consultation fee', 'insurance'],
    },
    'law_firm': {
        'tone':             "formal, precise, trustworthy, and cautious",
        'polish_hint':      "Formal register. Never give legal advice. Use passive voice sparingly.",
        'lead_keywords':    ['consultation', 'case review', 'speak to lawyer', 'legal advice'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['retainer', 'hourly rate', 'flat fee'],
    },
    'dental': {
        'tone':             "friendly, reassuring, and professional",
        'polish_hint':      "Reassure first, inform second. Avoid clinical jargon unless asked.",
        'lead_keywords':    ['appointment', 'booking', 'consultation', 'see a dentist'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['treatment cost', 'insurance', 'payment plan'],
    },
    'gym': {
        'tone':             "energetic, motivating, and supportive",
        'polish_hint':      "High energy, positive framing. Use active verbs.",
        'lead_keywords':    ['membership', 'sign up', 'trial', 'class', 'book a session'],
        'pricing_keywords': GLOBAL_PRICING_KW + ['membership fee', 'monthly', 'annual'],
    },
}
