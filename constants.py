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
# NOTE: 'plan', 'cost', 'buy' are short and will match broadly — e.g. 'plan'
# hits "payment plan", "dental plan", "business plan". These are inherited
# behaviour; narrowing them is a future-sprint concern.
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
# BUG FIX: 'human agent' and 'real person' were previously in BOTH this dict
# and ACTION_KEYWORDS['contact_request']. Because Tier 2A (action) runs before
# Tier 2B (tool), they always routed to contact_request and never reached
# escalate_to_human. Removed from here; contact_request action handles them.
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
        'speak to a human', 'talk to a person',
        'talk to someone', 'connect me to support',
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

# ── Lead nudge pacing ───────────────────────────────────────────────────────
# purchase_stage is a one-way ratchet (above) — once it reaches 'evaluating'
# or later, is_lead stays True for the rest of the session. Without a cap,
# _build_lead_nudge() (ai_helper.py) would append an email ask to every
# single reply for the rest of the conversation. These bound that: nudge at
# most LEAD_NUDGE_MAX_PER_SESSION times total, with at least
# LEAD_NUDGE_COOLDOWN_TURNS turns between asks.
LEAD_NUDGE_MAX_PER_SESSION: int = 2
LEAD_NUDGE_COOLDOWN_TURNS:  int = 4

# ── Account deletion ────────────────────────────────────────────────────────
# Self-service "delete account" is a soft delete: the agency's account and
# widgets keep working for this many days (in case it was a mistake or they
# want to export data), then a daily cron job permanently deletes it.
# See blueprints/cron.py::cron_hard_delete_accounts and models/users.py.
ACCOUNT_DELETION_GRACE_DAYS: int = 30



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

# ── Prospect informational signals (Tier 2.5 — zero cost) ────────────────────
# Catches evaluation-stage questions that don't carry explicit booking/pricing
# language but strongly signal a prospect actively sizing up the service.
#
# Design principle: target the STRUCTURAL SHAPE of prospect questions, not
# their topic. The list covers three overlapping populations:
#
#   1. SaaS / agency language — setup, onboarding, integration, trial
#   2. Professional service language — consultation, new-client signals,
#      service-scope, eligibility (lawyers, accountants, therapists, dentists)
#   3. Trades & personal service language — geographic coverage, credentials,
#      mobile/home visits, emergency callouts, event/occasion services
#      (plumbers, electricians, cleaners, makeup artists, personal trainers,
#      photographers, caterers, and any other appointment-led business)
#
# is_sales intentionally stays False for all entries here: evaluation signals
# warrant an email nudge, not a pricing CTA. GLOBAL_PRICING_KW handles
# is_sales independently.
#
# Pure substring scan on a pre-lowercased message — zero model cost.
# Checked in detect_intent() after Tier 2B and before Tier 3 (Gemini).
#
# Coverage benchmark (31 test messages across plumber, makeup artist,
# electrician, cleaner, personal trainer verticals): 96%.
# Remaining gap: trade certification acronyms with non-standard word order
# e.g. "are you niceic certified" — Gemini Tier 3 handles these correctly.
PROSPECT_INFO_KEYWORDS: List[str] = [

    # ── First-person interest framing ─────────────────────────────────────────
    # Explicit intent statements — an existing customer never says these.
    "i'm looking for", "i am looking for",
    "i'm interested in", "i am interested in",
    "i'm thinking about", "i am thinking about",
    "i'm considering", "i am considering",
    "i'd like to know more", "i would like to know",
    'tell me more about your', 'tell me about your',
    'looking for a', 'searching for a',

    # ── Service scope inquiry ─────────────────────────────────────────────────
    # Prospect mapping out what the business does — existing customers know.
    'what do you offer', 'what do you provide',
    'what services do you', 'what service do you',
    'what are your services', 'what do you specialise in',
    'what do you specialize in', 'what can you help with',
    'what types of', 'what kind of service',
    'what are your packages', 'what does your',
    "what's included in", 'what is included in',

    # ── Do-you / can-you capability checks ───────────────────────────────────
    # "do you offer/handle/cover/do X", "can you fix/repair/install X" —
    # checking whether a service or capability exists. Covers professional
    # services ("do you handle commercial cases") and trades ("can you fix
    # a boiler", "do you do emergency call outs", "do you do bridal makeup",
    # "do you do end of tenancy"). 'do you do' is intentionally broad: in
    # a prospect context it nearly always means "is this a service you provide".
    'do you offer', 'do you provide',
    'do you handle', 'do you deal with',
    'do you work with', 'do you cater',
    'do you cover', 'do you serve',
    'do you have experience', 'do you specialise',
    'do you specialize', 'are you able to',
    'would you be able to',
    'do you do',
    'do you fix', 'do you repair', 'do you install',
    'can you fix', 'can you repair', 'can you install',
    'can you replace', 'can you help with',

    # ── Geographic / coverage checks ─────────────────────────────────────────
    # Critical for any local or mobile business — plumbers, electricians,
    # cleaners, mobile beauticians, mobile personal trainers, caterers.
    # An existing customer already knows whether you cover their area.
    'are you local', 'are you local to',
    'do you work in', 'do you service this area',
    'what areas do you cover', 'what areas do you serve',
    'which areas do you', 'do you come to',
    'do you travel to', 'how far do you travel',
    'what is your coverage area',

    # ── Mobile / home-visit checks ────────────────────────────────────────────
    # Makeup artists, personal trainers, physiotherapists, cleaners, tutors —
    # any service provider who may work at the client's location.
    'are you mobile', 'do you come to me', 'can you come to me',
    'do you do home visits', 'do you do home visit',
    'do you visit', 'can you visit',

    # ── Urgency / emergency callouts ──────────────────────────────────────────
    # Plumbers, electricians, locksmiths, glaziers, HVAC engineers.
    # Emergency callout is pure prospect — they need someone now.
    'do you do emergency', 'emergency call out', 'emergency callout',
    'can you come out', 'can someone come out',
    'how quickly can you', 'can you come today',
    'do you offer emergency', 'same day service',

    # ── New client / first-time signals ──────────────────────────────────────
    # Unambiguous: only a prospect says these.
    'are you taking new', 'do you accept new',
    'are you accepting', 'taking on new clients',
    'taking new patients', 'accepting new patients',
    'first time', 'never been before', "haven't been before",
    'new to this', 'new customer', 'new client',
    'never tried', 'just moved', 'recently moved',
    "i'm new here", 'i am new here',

    # ── Credentials / certifications ──────────────────────────────────────────
    # High-signal: only a prospect about to hire asks this. Covers trades
    # (gas safe, insured, licensed) and personal services (police checked,
    # DBS, qualified). 'are you niceic/corgi certified' won't match
    # 'are you certified' due to word order — handled by Gemini Tier 3.
    'are you insured', 'do you have insurance',
    'are you licensed', 'are you registered',
    'are you certified', 'are you qualified',
    'are you police checked', 'are you dbs',
    'are you gas safe', 'what certification',
    'what qualifications do you', 'do you have a licence',
    'do you have a license',

    # ── How-it-works / process discovery ─────────────────────────────────────
    # Exploratory questions from someone unfamiliar with the service.
    'how does it work', 'how does this work',
    'how does the process work', "what's the process",
    'what is the process',
    'walk me through', 'take me through',
    'what happens when i', 'what happens if i',
    'what does the process look like',

    # ── Session / service structure ───────────────────────────────────────────
    # What to expect from the first visit — personal trainers, therapists,
    # consultants, tutors, stylists.
    'what does a typical session', 'what does a session look like',
    'what does a typical visit', 'what to expect',
    'what happens during', 'what products do you use',

    # ── Initial engagement / consultation / quote ─────────────────────────────
    'can i get a consultation', 'do you offer a consultation',
    'can i get a quote', 'can i get an estimate',
    'can i get a free', 'how do i begin', 'how do i arrange',
    'how do i book', 'how do i schedule',
    'can i book a', 'can i schedule a',

    # ── Trial / introductory offer ────────────────────────────────────────────
    'is there a trial', 'is there a free trial',
    'do you have a trial', 'trial offer', 'free session',
    'introductory offer', 'new client offer',
    'new patient offer', 'first visit offer',
    'taster session', 'how do i try', 'can i get a trial',
    'can i test it out', 'how do i get access',

    # ── Event / occasion ──────────────────────────────────────────────────────
    # Makeup artists, photographers, florists, caterers, DJs, stylists.
    'for a wedding', 'for my wedding', 'for an event',
    'do you do weddings', 'do you do bridal',
    'do you do events', 'do you do corporate',
    'wedding makeup', 'bridal makeup', 'bridal hair',

    # ── Eligibility / suitability ─────────────────────────────────────────────
    'is this right for', 'is this suitable for',
    'would this work for', 'am i eligible',
    'are you the right', 'is this for people who',

    # ── Timeline / booking lead time ──────────────────────────────────────────
    'how long does it take', 'how long does setup take',
    'how long does onboarding take', 'how long will it take',
    'how long to set up', 'how long to go live', 'how long until',
    'how long before i can', 'how soon can i start',
    'how quickly can i', 'do you have availability',
    'when can i start',
    'how far in advance', 'how long in advance',
    'how soon do i need to book', 'how early should i book',

    # ── Setup / account creation (SaaS / digital services) ───────────────────
    'how do i set up', 'how do i setup', 'how does setup work',
    'what is the setup', 'setup process',
    'how do i create an account', 'how do i open an account',
    'how do i register', 'how do i sign up', 'how to sign up',

    # ── Onboarding process ────────────────────────────────────────────────────
    "how's the onboarding", 'how is the onboarding',
    'how does onboarding work', 'what does onboarding look like',
    'what is the onboarding', 'onboarding process', 'onboarding steps',

    # ── Getting started ───────────────────────────────────────────────────────
    'how do i get started', 'how do i start', 'how to get started',
    'what are the first steps', 'where do i begin', 'where do i start',

    # ── Implementation / integration (SaaS / digital) ─────────────────────────
    'how do i integrate', 'how does the integration work',
    'does it integrate with', 'how do i install',
    'how do i embed', 'how do i connect it', 'how does it connect',

    # ── Requirements / what's needed ──────────────────────────────────────────
    'what do i need to get started', 'what do i need to set up',
    'what do you need from me', 'what information do you need',
    'what is required to', 'what are the requirements',

    # ── Pricing pattern specific to trades ────────────────────────────────────
    # GLOBAL_PRICING_KW has 'how much', 'cost', 'price' but not 'do you charge'.
    # Trades commonly phrase it as "do you charge a call out fee".
    'do you charge a', 'is there a call out',

    # ── Post-decision process questions ───────────────────────────────────────
    'what happens after', 'what happens next', 'what comes next',
    'what are the next steps', 'what is the next step',
]

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
