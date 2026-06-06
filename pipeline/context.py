"""
pipeline/context.py
===================
Shared context objects for the Lumvi response pipeline.

WHY THIS EXISTS
---------------
generate_response() currently passes state between its ~20 stages through
local variables: _call1_used, _rag_qualified, candidates, session_mem, etc.
This makes every stage depend on the entire function — you can't test or
replace a stage without understanding all the others.

PipelineRequest is the single context object that flows through every stage.
Each stage reads what it needs and writes what it produces — nothing more.
PipelineResult is the typed output that replaces the ad-hoc dict currently
returned by generate_response().

MIGRATION
---------
1. Import these into ai_helper.py — no other files change yet.
2. In generate_response(), build a PipelineRequest at the top and a
   PipelineResult at the bottom. Replace local variables with ctx.* attributes
   as you extract each stage.
3. app.py calls result.to_dict() — the response shape is unchanged.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Pipeline Request ──────────────────────────────────────────────────────────

@dataclass
class PipelineRequest:
    """
    All state that flows through the response pipeline.

    Divided into logical groups that mirror the pipeline stages:
      Inputs        — supplied by the caller (app.py)
      Preprocessing — set by the preprocess stage
      Session       — set by the session stage
      Intent        — set by the intent/action stages
      Retrieval     — set by the embedding search + rerank stages
      Scoring       — thresholds and confidence values
      Budget        — tracks Gemini call usage (hard limit: 2 per turn)
      Generation    — context string for the RAG stage
      Cache         — Redis response cache key
      Trace         — observability IDs and timing
    """

    # ── Inputs (supplied by caller) ───────────────────────────────────
    user_message: str
    faqs: List[Dict]
    vertical: str                          = 'general'
    conversation_history: List[Dict]       = field(default_factory=list)
    client_id: Optional[str]              = None
    lead_triggers: List[str]               = field(default_factory=list)
    kb_version: Optional[int]             = None
    session_id: Optional[str]             = None

    # ── Preprocessing ─────────────────────────────────────────────────
    # clean_message: whitespace-normalised version of user_message.
    # Set by the preprocess stage; all subsequent stages read this,
    # never user_message directly.
    clean_message: str                     = ''

    # ── Session ───────────────────────────────────────────────────────
    # Merged DB + regex session state. Set by the session stage.
    # Keys: name, email, phone, purchase_stage, frustration_score,
    #       is_frustrated, repeated_question, turns, turn_count,
    #       handoff_offered.
    session_mem: Dict                      = field(default_factory=dict)

    # ── Intent / Action ───────────────────────────────────────────────
    # Populated by the intent stage before retrieval runs.
    is_sales_query: bool                   = False
    detected_action: Optional[str]        = None  # e.g. 'demo_request'
    detected_tool: Optional[str]          = None  # e.g. 'lookup_order'

    # ── Retrieval ─────────────────────────────────────────────────────
    # search_query: rewritten/expanded query used for embedding search.
    # Starts as clean_message; may be rewritten by Call 1.
    search_query: str                      = ''

    # Raw results from _embedding_search (pre-rerank).
    candidates: List[Dict]                 = field(default_factory=list)
    vector_scores: List[float]             = field(default_factory=list)

    # Results after _hybrid_rerank (RRF). These are what RAG generation sees.
    hybrid_ranked: List[Dict]              = field(default_factory=list)
    hybrid_scores: List[float]             = field(default_factory=list)

    # Known-bad KB IDs from poor-answer feedback. Loaded once per request.
    poor_kb_ids: set                       = field(default_factory=set)

    # ── Scoring / confidence ──────────────────────────────────────────
    # top_cosine: raw cosine similarity of the top-ranked candidate.
    #   Used for confidence gating — never compare RRF scores to these thresholds.
    # top_hybrid: RRF score of the top-ranked candidate (for logging only).
    top_cosine: float                      = 0.0
    top_hybrid: float                      = 0.0

    # Confidence bands — set by the threshold stage from is_sales_query.
    # Sales queries get a 0.05 lower floor to avoid dropping price questions.
    vector_threshold: float                = 0.40   # minimum cosine for RAG
    confidence_high: float                 = 0.65   # answer confidently
    confidence_medium: float               = 0.40   # answer with hedge

    # Whether the top candidate clears the minimum RAG score gate.
    rag_qualified: bool                    = False

    # ── Budget tracking ───────────────────────────────────────────────
    # The pipeline allows MAX 2 Gemini calls per turn:
    #   call1: _combined_rewrite_intent (conditional)
    #   call2: _rag_generate_and_polish (always, when rag_qualified)
    # Stages must check call1_used before consuming a Gemini call.
    call1_used: bool                       = False

    # ── Generation ────────────────────────────────────────────────────
    # Conversation context string passed to the RAG prompt.
    # Built by _build_context() from history + optional DB summary.
    context_str: str                       = ''

    # ── Cache ─────────────────────────────────────────────────────────
    # SHA-256 key for the Redis response cache. Set at the start of
    # generate_response() so the write path uses the same key as the read path.
    resp_cache_key: Optional[str]         = None

    # ── Trace ─────────────────────────────────────────────────────────
    trace_id: str                          = field(
        default_factory=lambda: uuid.uuid4().hex[:8]
    )
    pipeline_start: float                  = field(
        default_factory=time.monotonic
    )

    # ── Convenience helpers ───────────────────────────────────────────

    @property
    def history(self) -> List[Dict]:
        """Alias for conversation_history — shorter to type in stage code."""
        return self.conversation_history

    @property
    def elapsed_ms(self) -> float:
        """Milliseconds since the pipeline started."""
        return (time.monotonic() - self.pipeline_start) * 1000

    @property
    def top_kb_id(self) -> str:
        """ID of the top-ranked hybrid candidate, or empty string."""
        if not self.hybrid_ranked:
            return ''
        c = self.hybrid_ranked[0]
        return str(c.get('kb_id', c.get('id', '')))


# ── Pipeline Result ───────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    """
    Typed output from the pipeline.

    to_dict() returns the same shape as the dict generate_response() currently
    returns, so app.py requires zero changes during migration.
    """

    response: str
    method: str
    confidence: float

    # Lead / action fields
    is_lead: bool                          = False
    lead_metadata: Optional[Dict]         = None
    action: Optional[Dict]                = None
    trigger_lead_collection: bool         = False

    # Handoff / clarification (optional — only set on specific code paths)
    handoff: Optional[Dict]               = None
    clarification: Optional[Dict]         = None
    needs_followup: bool                  = False

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize to the dict shape app.py expects from generate_response().

        Optional fields are only included when set to avoid breaking any
        app.py code that checks `'field' in result` rather than result.get().
        """
        d: Dict[str, Any] = {
            'response':      self.response,
            'method':        self.method,
            'confidence':    self.confidence,
            'is_lead':       self.is_lead,
            'lead_metadata': self.lead_metadata,
            'action':        self.action,
        }
        if self.handoff is not None:
            d['handoff'] = self.handoff
        if self.clarification is not None:
            d['clarification'] = self.clarification
        if self.needs_followup:
            d['needs_followup'] = self.needs_followup
        if self.trigger_lead_collection:
            d['trigger_lead_collection'] = self.trigger_lead_collection
        return d

    # ── Common result constructors ────────────────────────────────────
    # These replace the many inline dict literals in generate_response().
    # Using named constructors makes the intent of each return path obvious.

    @classmethod
    def empty_message(cls) -> 'PipelineResult':
        return cls(
            response='How can I help you today?',
            method='empty',
            confidence=1.0,
        )

    @classmethod
    def cache_hit(cls, cached: Dict) -> 'PipelineResult':
        return cls(
            response=cached['response'],
            method='cache',
            confidence=cached.get('confidence', 0.8),
        )

    @classmethod
    def fatal_error(cls) -> 'PipelineResult':
        return cls(
            response=(
                "I'm sorry \u2014 something went wrong while processing your request. "
                "Please try again in a moment."
            ),
            method='fatal_fallback',
            confidence=0.0,
        )

    @classmethod
    def idk(cls) -> 'PipelineResult':
        return cls(
            response=(
                "I don't have enough information to answer that accurately. "
                "Would you like me to connect you with the team?"
            ),
            method='idk_fallback',
            confidence=0.0,
        )

    @classmethod
    def declined_handoff(cls) -> 'PipelineResult':
        return cls(
            response="No problem! Is there anything else I can help you with?",
            method='declined_handoff',
            confidence=1.0,
        )

    @classmethod
    def goodbye(cls) -> 'PipelineResult':
        return cls(
            response="Thanks for chatting! Feel free to come back anytime. \U0001f44b",
            method='goodbye',
            confidence=1.0,
        )

    @classmethod
    def gratitude(cls) -> 'PipelineResult':
        return cls(
            response="You're welcome! Is there anything else I can help with?",
            method='gratitude',
            confidence=1.0,
        )
