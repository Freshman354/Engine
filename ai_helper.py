"""
ai_helper.py  (refactored)
==========================
Thin AIHelper class. Responsibilities:
  1. Gemini model initialisation
  2. generate_response() — pipeline orchestrator (delegates to stages)
  3. KB management methods (enrich_and_chunk, index_faqs, etc.)
  4. Module-level re-exports for backward compatibility with app.py

What moved out:
  Pure math          → pipeline/stages/math_helpers.py
  Retrieval          → pipeline/stages/retrieval.py
  Generation         → pipeline/stages/generation.py
  Intent / tools     → pipeline/stages/intent.py
  Escalation         → pipeline/stages/escalation.py
  Session extraction → services/session_store.py
  KB gap tracking    → services/kb_gap.py
  Embedding / cache  → services/embedding.py
  Shared constants   → constants.py

app.py requires zero changes — all public names remain importable from here.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import logging
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import google.genai as genai

# ── New module imports ────────────────────────────────────────────────────────
from constants import (
    BARE_PRONOUNS,
    GLOBAL_PRICING_KW,
    IDK_METHODS_ALL,
    INFERENCE_EXPANSION_ENABLED,
    PENDING_TOOL_ACTION_TTL_SECONDS,
    PERSONALITIES,
    STAGE_SIGNALS,
    STAGE_ORDER,
)
from pipeline.context import PipelineRequest, PipelineResult
from pipeline.stages.escalation  import check_escalation
from pipeline.stages.math_helpers import topic_overlap
from pipeline.stages.generation  import (
    build_context,
    dynamic_fallback,
    guardrails,
    make_fallback,
    maybe_summarise,
    parse_clarify_response,
    rag_generate_and_polish,
)
from pipeline.stages.intent import (
    detect_intent,
    detect_action_intent,
    detect_simple_intent,
    dispatch_tool,
    extract_tool_args,
    handle_detected_action,
    booking_redirect_message,
    order_cancellation_redirect_message,
    build_confirmation_prompt,
    build_missing_args_prompt,
    detect_confirmation,
    is_write_tool,
    missing_required_args,
    resolve_slot_selection,
)
from pipeline.stages.retrieval   import (
    bm25_only_search,
    cross_encoder_rerank,
    decompose_intents,
    embedding_search,
    hybrid_rerank,
    is_followup,
    last_response_category,
    resolve_query,
    rewrite_query,
)
from services.embedding    import embed as _embed, normalize as _normalize
from services.kb_gap       import (
    get_poor_answers,
    get_top_kb_gaps,
    record_kb_gap,
    record_poor_answer,
    send_kb_gap_digest,
)
from services.session_store import (
    clear_chat_session,
    extract_session_memory,
    load_chat_session,
    persist_session,
)
from utils import get_logger, log_crash, generate as _gemini_call

logger = get_logger('lumvi.ai_helper')

# ── Background task executor ──────────────────────────────────────────────────
_BG_EXECUTOR: concurrent.futures.ThreadPoolExecutor = (
    concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix='lumvi_bg')
)


def _log_bg_exception(future: 'concurrent.futures.Future') -> None:
    """
    FIX: every _bg_submit() call in this file was fire-and-forget —
    if the submitted function raised, the exception sat in the Future
    object and was never retrieved (nothing ever called .result() on these
    futures), so it was never logged anywhere. A failed session persist, a
    failed lead notification, a failed cache write — all silent. This
    callback surfaces any background-task exception to the logs.
    """
    exc = future.exception()
    if exc is not None:
        logger.error(f"[Background] task failed: {exc!r}", exc_info=exc)


def _bg_submit(fn, *args, **kwargs) -> 'concurrent.futures.Future':
    """
    Drop-in replacement for _BG_EXECUTOR.submit() that also logs failures.

    FIX (critical): this called itself — `_bg_submit(fn, *args, **kwargs)`
    inside the body of `_bg_submit` — instead of `_BG_EXECUTOR.submit(...)`.
    Every one of the ~25 call sites in this file (session persistence,
    lead notification, cache writes, summarisation) would hit
    RecursionError the instant it ran. This is not a degraded-path bug —
    it's a guaranteed crash on essentially every conversation turn that
    reaches any background task.
    """
    future = _BG_EXECUTOR.submit(fn, *args, **kwargs)
    future.add_done_callback(_log_bg_exception)
    return future

# ── Response-level Redis cache ────────────────────────────────────────────────
_REDIS_RESP_PREFIX  = 'lumvi:resp:v1:'
_REDIS_RESP_TTL_SEC = 3600  # 1 hour

_redis_resp_client: Optional[Any] = None
try:
    import redis as _redis_lib
    _redis_url = os.environ.get('REDIS_URL')
    if _redis_url:
        _redis_resp_client = _redis_lib.from_url(
            _redis_url,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        logger.info("[AIHelper] Redis response cache enabled")
except Exception as _rc_err:
    logger.info(f"[AIHelper] Redis response cache unavailable: {_rc_err}")

# ── Singleton ─────────────────────────────────────────────────────────────────
_ai_helper_instance: Optional['AIHelper'] = None
_ai_helper_lock     = threading.Lock()

# ── Lead extraction — regex (no Gemini call) ──────────────────────────────────
_EMAIL_RE   = re.compile(
    # FIX: was r'\b[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}\b' — the domain part only
    # allowed ONE label before the TLD, so it silently truncated any
    # compound-TLD address: john@business.co.uk was captured as
    # "john@business.co", sarah@company.com.au as "sarah@company.com" —
    # both undeliverable. (?:[\w-]+\.)+ now allows one or more dotted
    # labels before the final all-letter TLD.
    r'\b[\w.+-]+@(?:[\w-]+\.)+[a-zA-Z]{2,}\b'
)
_PHONE_RE   = re.compile(r'\b(?:\+?\d[\d\s\-().]{7,14}\d)\b')
_NAME_RE    = re.compile(
    # FIX: was anchored to [A-Z][a-z]+, so lowercase chat input like
    # "hi i'm sarah" silently failed to capture a name. Now case-insensitive;
    # callers should .title() the captured group if they need display casing.
    r"(?:i'?m|my name is|this is|call me)\s+([A-Za-z]+(?:\s+[A-Za-z]+)?)",
    re.IGNORECASE,
)
_URGENT_KW  = frozenset({'urgent', 'asap', 'immediately', 'today', 'right now', 'right away'})

# ── IDK response cache TTL ────────────────────────────────────────────────────
_REDIS_IDK_TTL_SEC = 900   # 15 minutes

# Minimum BM25 score to "qualify" for generation when running in the
# embeddings-unavailable fallback path (see bm25_only_search in
# retrieval.py). BM25 scores are NOT on the same 0-1 scale as cosine
# similarity, so this is intentionally a separate, low bar — the goal of
# this path is "don't go fully dark", not "match RAG's normal precision".
# Tune once real score distributions from this corpus are visible.
_BM25_FALLBACK_MIN_SCORE = 0.5

# ── Lead nudge — vertical-specific capture prompts ────────────────────────────
_VERTICAL_NUDGES: Dict[str, str] = {
    'real_estate': (
        "Would you like one of our agents to reach out? "
        "Drop your email and we'll be in touch."
    ),
    'law_firm': (
        "If you'd like to arrange a consultation, share your email "
        "and a member of the team will reach out to discuss your situation."
    ),
    'healthcare': (
        "If you'd like to discuss this further, share your email "
        "and someone from the team will be in touch."
    ),
    'dental': (
        "Happy to arrange a consultation — what email should we use "
        "to get in touch with you?"
    ),
    'saas': (
        "Would you like someone from the team to walk you through it? "
        "Share your email and we'll set something up."
    ),
    'gym': (
        "Want us to reach out about membership options? "
        "Drop your email and we'll be in touch."
    ),
    'fitness': (
        "Want us to reach out about membership options? "
        "Drop your email and we'll be in touch."
    ),
}

# ── Purchase stage detection ──────────────────────────────────────────────────
def _detect_purchase_stage(msg: str) -> Optional[str]:
    msg_l = msg.lower()
    for stage, signals in STAGE_SIGNALS.items():
        if any(s in msg_l for s in signals):
            return stage
    return None


def _advance_stage(current: Optional[str], detected: Optional[str]) -> Optional[str]:
    if not detected:
        return current
    if not current:
        return detected
    try:
        if STAGE_ORDER.index(detected) >= STAGE_ORDER.index(current):
            return detected
    except ValueError:
        pass
    return current


def get_ai_helper(
    api_key:    Optional[str] = None,
    model_name: Optional[str] = None,
) -> 'AIHelper':
    global _ai_helper_instance
    if _ai_helper_instance is None:
        with _ai_helper_lock:
            if _ai_helper_instance is None:
                _ai_helper_instance = AIHelper(
                    api_key    = api_key    or os.environ.get('GEMINI_API_KEY', ''),
                    model_name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash'),
                )
    return _ai_helper_instance


# ═════════════════════════════════════════════════════════════════════════════
# AIHelper
# ═════════════════════════════════════════════════════════════════════════════

class AIHelper:

    def __init__(
        self,
        api_key:    str = '',
        model_name: str = 'gemini-2.0-flash',
    ) -> None:
        self.api_key    = api_key
        self.model_name = model_name
        self.enabled    = bool(api_key)
        self.model: Optional[Any] = None

        if self.enabled:
            try:
                self._genai_client = genai.Client(api_key=api_key)
                self.model = self._genai_client.models
                self._model_name = model_name
                logger.info(f"[AIHelper] google.genai ready model={model_name}")
            except Exception as e:
                log_crash(logger, 'AIHelper/init', e, model=model_name)
                self.enabled = False
                self.model = None
        else:
            logger.warning(
                "[AIHelper] GEMINI_API_KEY not set — running in static fallback mode. "
                "Retrieval (embedding search) still works; LLM generation disabled."
            )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _cache_key(self, text: str, *parts: str) -> str:
        raw = ':'.join([text, *parts])
        return hashlib.sha256(raw.encode()).hexdigest()

    def _parse_json(self, text: str) -> Optional[Dict]:
        try:
            clean = text.strip().strip('`')
            if clean.lower().startswith('json'):
                clean = clean[4:].strip()
            return json.loads(clean)
        except Exception:
            return None

    def _read_resp_cache(self, cache_key: str) -> Optional[Dict]:
        if not _redis_resp_client:
            return None
        try:
            raw = _redis_resp_client.get(_REDIS_RESP_PREFIX + cache_key)
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def _write_resp_cache(self, cache_key: str, result: Dict) -> None:
        if not _redis_resp_client:
            return
        try:
            _redis_resp_client.setex(
                _REDIS_RESP_PREFIX + cache_key,
                _REDIS_RESP_TTL_SEC,
                json.dumps(result),
            )
        except Exception:
            pass

    def _write_resp_cache_ttl(self, cache_key: str, result: Dict, ttl: int) -> None:
        if not _redis_resp_client:
            return
        try:
            _redis_resp_client.setex(
                _REDIS_RESP_PREFIX + cache_key,
                ttl,
                json.dumps(result),
            )
        except Exception:
            pass

    def _build_handoff_payload(
        self,
        history: List[Dict],
        session_mem: Dict,
        last_question: str,
        session_id: Optional[str],
        trigger_method: str = 'bot',
    ) -> Dict:
        return {
            'trigger':         trigger_method,
            'session_id':      session_id,
            'last_question':   last_question,
            'name':            session_mem.get('name'),
            'email':           session_mem.get('email'),
            'phone':           session_mem.get('phone'),
            'purchase_stage':  session_mem.get('purchase_stage'),
            'turn_count':      session_mem.get('turn_count', 0),
            'is_frustrated':   session_mem.get('is_frustrated', False),
        }

    def _extract_lead_info(
        self,
        message: str,
        session_mem: Dict,
        vertical: str,
        lead_triggers: List[str],
    ) -> Dict:
        result: Dict = {}
        m = _EMAIL_RE.search(message)
        result['email'] = m.group(0) if m else session_mem.get('email')
        m = _PHONE_RE.search(message)
        result['phone'] = m.group(0).strip() if m else session_mem.get('phone')
        m = _NAME_RE.search(message)
        result['name'] = m.group(1).strip().title() if m else session_mem.get('name')
        msg_lower = message.lower()
        result['urgency'] = 'high' if any(kw in msg_lower for kw in _URGENT_KW) else 'medium'
        result['purchase_stage'] = session_mem.get('purchase_stage')
        return {k: v for k, v in result.items() if v}

    def _build_lead_nudge(
        self,
        lead_info: Dict,
        vertical: str,
        purchase_stage: Optional[str] = None,
        is_sales: bool = False,
        lead_q3: str = '',
    ) -> str:
        if lead_info.get('email'):
            if lead_q3:
                return lead_q3
            return (
                "I've noted your details — someone from the team will be in "
                "touch shortly. Is there anything else I can help with in the "
                "meantime?"
            )
        if purchase_stage in ('buying', 'evaluating') or is_sales:
            return (
                "To make sure the right person follows up with you — "
                "what's the best email address for you?"
            )
        if vertical in _VERTICAL_NUDGES:
            return _VERTICAL_NUDGES[vertical]
        return (
            "If you'd like someone to follow up, I can pass your details to "
            "the team — what's the best email to reach you on?"
        )

    def _build_email_request(self, vertical: str) -> str:
        """
        Post-acceptance email request — fires after the user says yes to a
        handoff but before we have their email. Distinct from _build_lead_nudge
        (which is a proactive soft ask). Here the handoff is committed; we just
        need the contact detail to complete it.
        """
        _templates = {
            'dental':           "Great — what email should we send your appointment confirmation to?",
            'healthcare':       "Of course — what email should the team use to get in touch with you?",
            'law_firm':         "Certainly — what email should a member of the team use to reach you?",
            'real_estate':      "Excellent — what email can we send the property details to?",
            'restaurant':       "Perfect — what email should we use to confirm your reservation?",
            'fitness':          "Let's do it — what email should we send the membership details to?",
            'gym':              "Let's do it — what email should we send the membership details to?",
            'beauty':           "Lovely — what email should we use to confirm your booking?",
            'automotive':       "Great — what email should we send the details to?",
            'therapy':          "Of course — what email should the team use to reach out to you?",
            'mortgage':         "Excellent — what email should your adviser use to get in touch?",
            'insurance':        "Great — what email can we use to send you the details?",
            'accounting':       "Perfect — what email should the team use to get in touch?",
            'physiotherapy':    "Great — what email should we use to confirm your assessment?",
            'veterinary':       "Of course — what email should we use to confirm your appointment?",
            'optician':         "Great — what email should we send the details to?",
            'cleaning_services':"Perfect — what email can we use to confirm your booking?",
            'construction':     "Great — what email should we send the quote to?",
            'interior_design':  "Wonderful — what email should we use to send the brief across?",
            'childcare':        "Of course — what email should we use to get in touch with you?",
        }
        return _templates.get(
            vertical,
            "What's the best email address to reach you on?",
        )

    # ── Gap 3: Lead persistence and notification ──────────────────────────────

    def _persist_lead_and_notify(
        self,
        client_id: str,
        payload: Dict,
        vertical: str = 'general',
    ) -> None:
        """
        Write lead to DB via save_lead() and fire notification to end client.
        Best-effort — never raises. Always called via _BG_EXECUTOR so it
        never blocks the pipeline response.

        Notification priority:
          1. Email  — fires if clients.notification_email is set
          2. SMS    — fires if clients.notification_phone is set (Twilio)
          3. Webhook — fires if webhook_configs row exists for 'lead_captured'
        """
        if not client_id:
            return

        email = payload.get('email')
        phone = payload.get('phone')
        # FIX: was `if not email: return` — silently dropped every lead
        # who gave a phone number but no email ("just call me at ..."),
        # with no saved record and no notification to the business at all.
        # A lead is persistable if we have EITHER way to reach them; the
        # SMS notification path below already exists specifically for
        # phone-first businesses, so this was leaving that capability
        # unreachable in practice.
        if not email and not phone:
            logger.debug(
                f"[LeadPersist] no email or phone in payload, skipping client={client_id}"
            )
            return

        try:
            from models.leads import save_lead
            from models.clients import get_client_by_id

            lead_data = {
                'name':    payload.get('name') or '',
                'email':   email or '',
                'phone':   phone or '',
                'message': payload.get('last_question') or '',
                'conversation_snippet': '',
                'intent_summary': '',
                'priority': 'high' if payload.get('is_frustrated') else 'med',
            }

            saved = save_lead(client_id, lead_data)
            if not saved:
                logger.warning(f"[LeadPersist] save_lead returned False client={client_id}")
                return

            logger.info(
                f"[LeadPersist] saved client={client_id} "
                f"contact={email or phone} stage={payload.get('purchase_stage')}"
            )

            # ── Fetch delivery settings ───────────────────────────────
            client = get_client_by_id(client_id)
            if not client:
                return

            notif_email = client.get('notification_email')
            notif_phone = client.get('notification_phone')

            if notif_email:
                self._send_lead_email(notif_email, client, lead_data, vertical)

            if notif_phone:
                self._send_lead_sms(notif_phone, client, lead_data)

            # Webhook delivery is handled by the existing webhook_configs
            # infrastructure — fire it here if the table has a row for
            # this client and the 'lead_captured' event.
            self._fire_lead_webhook(client_id, lead_data)

        except Exception as e:
            log_crash(logger, 'LeadPersist', e, client_id=client_id)

    def _send_lead_email(
        self,
        to_email: str,
        client: Dict,
        lead_data: Dict,
        vertical: str = 'general',
    ) -> None:
        """
        Send lead notification email to the end client (the plumber, dentist, etc.).
        White-label: from_name uses branded_email_from or company_name — never 'Lumvi'.
        Plug in your email provider below (Resend, SendGrid, etc.).
        """
        try:
            from models.clients import get_email_from_for_client
            # Check for agency verified custom domain first (white-label).
            # Display name: branded_email_from (wl_email_from) is the single
            # source of truth — custom domain only changes the sending address.
            # This avoids the two-store sync problem where agency_email_domains
            # .from_name could be stale if wl_email_from was updated after
            # the domain was saved.
            from models.agency_domains import get_verified_domain_for_client as _get_vd
            _custom = _get_vd(client['client_id'])

            # Display name: wl_email_from (branded_email_from) → company_name → fallback
            # Same regardless of whether a custom domain is active.
            _branded = client.get('branded_email_from', '').strip()
            from_name = _branded or client.get('company_name', '') or 'Your Chatbot'

            # Sending address: custom domain → Lumvi default
            if _custom:
                _smtp_from_addr = _custom.get('from_email') or os.environ.get('MAIL_FROM', 'support@lumvi.net')
            else:
                _smtp_from_addr = os.environ.get('MAIL_FROM', 'support@lumvi.net')

            name    = lead_data.get('name') or 'Someone'
            email   = lead_data.get('email', '')
            phone   = lead_data.get('phone') or '—'
            message = lead_data.get('message') or '—'

            # FIX: phone-only leads (no email) previously produced a broken
            # empty mailto: link and an empty Reply-To header. Build the
            # email row and the "how to follow up" line so a phone-only
            # lead reads cleanly instead of pointing at a dead link.
            has_email    = bool(email)
            email_display = (
                f'<a href="mailto:{email}" style="color:#B8924A;">{email}</a>'
                if has_email else '—'
            )
            follow_up_line = (
                f"Reply directly to {email} to follow up." if has_email
                else f"No email on file — follow up by phone at {phone}."
            )

            subject = f"New lead from your chatbot — {name}"

            html = f"""
<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px;color:#1C1917;">
  <p style="font-size:18px;font-weight:bold;margin-bottom:4px;">New lead from your chatbot</p>
  <p style="color:#78716C;margin-top:0;font-size:13px;">Someone reached out and we captured their details.</p>
  <hr style="border:none;border-top:1px solid #E7E2DA;margin:20px 0;">
  <table style="width:100%;border-collapse:collapse;">
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;width:90px;">Name</td>
        <td style="padding:8px 0;font-weight:600;">{name}</td></tr>
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;">Email</td>
        <td style="padding:8px 0;">{email_display}</td></tr>
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;">Phone</td>
        <td style="padding:8px 0;">{phone}</td></tr>
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;vertical-align:top;">Message</td>
        <td style="padding:8px 0;font-style:italic;">&ldquo;{message[:300]}&rdquo;</td></tr>
  </table>
  <hr style="border:none;border-top:1px solid #E7E2DA;margin:20px 0;">
  <p style="font-size:13px;color:#78716C;">
    {follow_up_line}
  </p>
</body>
</html>"""

            text = (
                f"New lead from your chatbot\n\n"
                f"Name:    {name}\n"
                f"Email:   {email or '—'}\n"
                f"Phone:   {phone}\n"
                f"Message: {message[:300]}\n\n"
                f"{follow_up_line}"
            )

            # ── SMTP delivery (Brevo via Flask-Mail env vars) ────────────
            # Uses the same env vars Flask-Mail reads in app.py so no new
            # Railway variables are needed — MAIL_USERNAME and MAIL_PASSWORD
            # are already set for Brevo (smtp-relay.brevo.com).
            # MAIL_FROM: the verified sender address on your Brevo account.
            # The white-label display name (from_name) comes from
            # get_email_from_for_client(), so the end client sees
            # "From: Acme Digital Agency <support@lumvi.net>" — not Lumvi.
            import smtplib
            from email.mime.multipart import MIMEMultipart as _MIME
            from email.mime.text      import MIMEText      as _MIMEText

            _smtp_host      = os.environ.get('MAIL_SERVER', 'smtp-relay.brevo.com')
            _smtp_port      = int(os.environ.get('MAIL_PORT', 587))
            _smtp_user      = os.environ.get('MAIL_USERNAME')
            _smtp_pass      = os.environ.get('MAIL_PASSWORD')

            if not all([_smtp_host, _smtp_user, _smtp_pass]):
                logger.warning(
                    '[LeadEmail] SMTP not configured — ensure MAIL_SERVER, '
                    'MAIL_USERNAME, MAIL_PASSWORD are set in Railway env vars'
                )
                return

            msg             = _MIME('alternative')
            msg['Subject']  = subject
            msg['From']     = f'{from_name} <{_smtp_from_addr}>'
            msg['To']       = to_email
            # FIX: was unconditionally `msg['Reply-To'] = email` — for a
            # phone-only lead (email='') this set an empty/invalid header.
            # Only set Reply-To when there's an actual email to reply to.
            if has_email:
                msg['Reply-To'] = email
            msg.attach(_MIMEText(text, 'plain'))
            msg.attach(_MIMEText(html,  'html'))

            with smtplib.SMTP(_smtp_host, _smtp_port) as _srv:
                _srv.ehlo()
                _srv.starttls()
                _srv.login(_smtp_user, _smtp_pass)
                _srv.sendmail(_smtp_from_addr, to_email, msg.as_string())

            logger.info(f"[LeadEmail] sent to={to_email} client={client.get('client_id')}")

        except Exception as e:
            log_crash(logger, 'LeadEmail', e, to=to_email, client_id=client.get('client_id'))

    def _send_lead_sms(
        self,
        to_phone: str,
        client: Dict,
        lead_data: Dict,
    ) -> None:
        """
        Send lead SMS to end client's mobile. Fires within seconds of capture.
        Requires TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER env vars.
        No-ops gracefully when Twilio is not configured.
        """
        try:
            sid   = os.environ.get('TWILIO_ACCOUNT_SID')
            token = os.environ.get('TWILIO_AUTH_TOKEN')
            from_ = os.environ.get('TWILIO_FROM_NUMBER')

            if not all([sid, token, from_]):
                return  # SMS not configured — skip silently

            name  = lead_data.get('name') or 'Someone'
            email = lead_data.get('email', '')
            phone = lead_data.get('phone') or ''

            body = (
                f"New chatbot lead\n\n"
                f"{name}\n"
                f"{email}"
                + (f"\n{phone}" if phone else '') +
                f"\n\nReply to follow up."
            )

            from twilio.rest import Client as TwilioClient
            TwilioClient(sid, token).messages.create(
                body=body[:1600],
                from_=from_,
                to=to_phone,
            )
            logger.info(f"[LeadSMS] sent to={to_phone} client={client.get('client_id')}")

        except Exception as e:
            log_crash(logger, 'LeadSMS', e, to=to_phone, client_id=client.get('client_id'))

    def _fire_lead_webhook(
        self,
        client_id: str,
        lead_data: Dict,
    ) -> None:
        """
        Dispatch lead payload to any configured webhooks for this client.
        Uses the existing webhook_configs / webhook_logs infrastructure.
        No-ops if the client has no active webhook configs for lead_captured.

        Function names sourced from models/__init__.py exports:
          get_webhooks(client_id)      — returns all webhook_configs rows for client
          log_webhook_delivery(...)    — inserts into webhook_logs
        """
        try:
            import json as _json
            import requests
            import hmac
            import hashlib as _hl
            from models import get_webhooks, log_webhook_delivery

            all_webhooks = get_webhooks(client_id)
            if not all_webhooks:
                return

            # Filter to active configs that include the lead_captured event
            active = [
                w for w in all_webhooks
                if w.get('enabled') and 'lead_captured' in (
                    _json.loads(w.get('events') or '[]')
                    if isinstance(w.get('events'), str)
                    else (w.get('events') or [])
                )
            ]
            if not active:
                return

            payload_str = _json.dumps({
                'event':      'lead_captured',
                'client_id':  client_id,
                'lead':       lead_data,
                'timestamp':  time.time(),
            })

            for wh in active:
                secret = wh.get('signing_secret') or ''
                sig    = hmac.new(
                    secret.encode() if secret else b'',
                    payload_str.encode(),
                    _hl.sha256,
                ).hexdigest()

                try:
                    t0 = time.time()
                    r  = requests.post(
                        wh['url'],
                        data=payload_str,
                        headers={
                            'Content-Type':      'application/json',
                            'X-Lumvi-Signature': f'sha256={sig}',
                            'X-Lumvi-Event':     'lead_captured',
                        },
                        timeout=10,
                    )
                    ms = int((time.time() - t0) * 1000)
                    log_webhook_delivery(
                        client_id, wh['webhook_id'], 'lead_captured',
                        wh['url'], payload_str,
                        r.status_code, r.text[:500], r.ok, ms,
                    )
                except Exception as req_err:
                    log_crash(logger, 'Webhook/request', req_err,
                              client_id=client_id, url=wh.get('url'))

        except Exception as e:
            log_crash(logger, 'LeadWebhook', e, client_id=client_id)

    # ── generate_response ─────────────────────────────────────────────────────

    def generate_response(
        self,
        user_message:          str,
        faqs:                  List[Dict],
        vertical:              str = 'general',
        conversation_history:  Optional[List[Dict]] = None,
        client_id:             Optional[str] = None,
        lead_triggers:         Optional[List[str]] = None,
        kb_version:            Optional[int] = None,
        session_id:            Optional[str] = None,
        lead_q3:               str = '',
        product_recommendations_enabled: bool = True,
    ) -> Dict:

        conversation_history = conversation_history or []
        lead_triggers        = lead_triggers or []

        # ── Empty message ─────────────────────────────────────────────
        if not user_message or not user_message.strip():
            return PipelineResult.empty_message().to_dict()

        # ── Build pipeline context ────────────────────────────────────
        ctx = PipelineRequest(
            user_message=user_message,
            faqs=faqs,
            vertical=vertical,
            conversation_history=conversation_history,
            client_id=client_id,
            lead_triggers=lead_triggers,
            kb_version=kb_version,
            session_id=session_id,
            product_recommendations_enabled=product_recommendations_enabled,
        )

        # ── Response cache key ─────────────────────────────────────────
        # FIX: the cache READ used to happen right here, before session
        # state was even loaded. That meant a state-dependent reply like
        # "yes" or "no" — which means something completely different
        # depending on whether a handoff/email-capture was just offered —
        # could be served a stale cached answer from a totally unrelated
        # conversation, silently bypassing the state machine below. The key
        # is still computed here (cheap), but the actual read is deferred
        # until after the email-capture / handoff / escalation checks have
        # had a chance to run — see "Response cache read" further down.
        ctx.resp_cache_key = self._cache_key(
            user_message.lower().strip(),
            client_id or '',
            vertical,
            str(kb_version or ''),
        )
        _top_cached: Optional[Dict] = None

        try:
            # ── Preprocess ────────────────────────────────────────────
            ctx.clean_message = re.sub(r'\s+', ' ', user_message.strip())

            # ── Session loading ───────────────────────────────────────
            db_session: Dict = {}
            if client_id and session_id:
                db_session = load_chat_session(client_id, session_id)

            regex_session = extract_session_memory(conversation_history, ctx.clean_message)
            ctx.session_mem = {**db_session, **regex_session}

            # ── Purchase stage detection (one-way ratchet) ────────────
            _detected_stage = _detect_purchase_stage(ctx.clean_message)
            _new_stage = _advance_stage(
                ctx.session_mem.get('purchase_stage'), _detected_stage
            )
            if _new_stage:
                ctx.session_mem['purchase_stage'] = _new_stage

            # ── Email capture (waiting for email after handoff acceptance) ──
            # Checked BEFORE handoff_offered — a user replying "sure, sarah@email.com"
            # contains both an affirmative and an email. Without this guard the
            # handoff_offered arm would misroute it as a fresh handoff acceptance.
            if ctx.session_mem.get('email_capture_pending'):
                msg_lower   = ctx.clean_message.lower()
                email_match = _EMAIL_RE.search(ctx.clean_message)
                phone_match = _PHONE_RE.search(ctx.clean_message)

                # User changed their mind
                if any(k in msg_lower for k in [
                    'no', 'nope', 'never mind', 'cancel', "don't", 'skip', 'forget it'
                ]):
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    ctx.session_mem['email_capture_retries']  = 0
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult.declined_handoff().to_dict()

                if email_match:
                    # Email captured — complete handoff and persist lead
                    ctx.session_mem['email']                 = email_match.group(0)
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    ctx.session_mem['email_capture_retries']  = 0
                    handoff_payload = self._build_handoff_payload(
                        conversation_history, ctx.session_mem,
                        ctx.clean_message, session_id, 'accepted'
                    )
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    _bg_submit(
                        self._persist_lead_and_notify,
                        client_id, handoff_payload, vertical
                    )
                    return PipelineResult(
                        response=(
                            f"Perfect — someone from the team will be in touch "
                            f"at {email_match.group(0)} shortly."
                        ),
                        method='handoff_accepted',
                        confidence=1.0,
                        handoff=handoff_payload,
                    ).to_dict()

                # FIX: previously any reply here that wasn't an email or a
                # "cancel" word fell straight to the re-ask below — so a
                # customer who replied with a phone number instead (a
                # completely reasonable thing to offer) just got asked for
                # an email again. Accept a phone number as a valid
                # alternative way to complete the handoff.
                if phone_match:
                    ctx.session_mem['phone']                 = phone_match.group(0).strip()
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    ctx.session_mem['email_capture_retries']  = 0
                    handoff_payload = self._build_handoff_payload(
                        conversation_history, ctx.session_mem,
                        ctx.clean_message, session_id, 'accepted'
                    )
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    _bg_submit(
                        self._persist_lead_and_notify,
                        client_id, handoff_payload, vertical
                    )
                    return PipelineResult(
                        response=(
                            f"Got it — someone from the team will give you a call "
                            f"at {ctx.session_mem['phone']} shortly."
                        ),
                        method='handoff_accepted',
                        confidence=1.0,
                        handoff=handoff_payload,
                    ).to_dict()

                # FIX: added a retry cap. Previously there was no counter at
                # all, so a customer who kept replying with something that
                # wasn't an email or a phone number could be re-asked for an
                # email indefinitely, forever, with no way out except the
                # exact words checked above. After 2 failed attempts, back
                # off gracefully instead of looping.
                _retries = ctx.session_mem.get('email_capture_retries', 0) + 1
                if _retries >= 2:
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    ctx.session_mem['email_capture_retries']  = 0
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult(
                        response=(
                            "No worries — I'll pass along that you'd like to connect, "
                            "and the team will look for the best way to follow up. "
                            "Anything else I can help with?"
                        ),
                        method='email_capture_gaveup',
                        confidence=1.0,
                    ).to_dict()

                ctx.session_mem['email_capture_retries'] = _retries
                _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                # Couldn't parse email or phone — re-ask once more, stay in capture state
                return PipelineResult(
                    response=(
                        "I didn't quite catch that — could you type out your email "
                        "address, or a phone number if that's easier?"
                    ),
                    method='email_capture_retry',
                    confidence=1.0,
                ).to_dict()

            # ── Handoff state machine ─────────────────────────────────
            if ctx.session_mem.get('handoff_offered'):
                msg_lower = ctx.clean_message.lower()
                if any(k in msg_lower for k in ['yes', 'yeah', 'sure', 'please', 'ok', 'go ahead']):

                    # Check if email was included in the same message
                    # e.g. "yes please, it's sarah@email.com"
                    email_in_msg = _EMAIL_RE.search(ctx.clean_message)
                    if email_in_msg:
                        ctx.session_mem['email'] = email_in_msg.group(0)

                    # FIX: was email-only (`_have_email = bool(session_mem
                    # .get('email'))`), so a lead who'd already given a
                    # phone number in an earlier turn still got pivoted into
                    # an email-capture prompt here, asking for contact info
                    # we already had — the same email-only assumption fixed
                    # at the other two persistence gates in this file, just
                    # missed at this third, structurally separate one.
                    _have_email = bool(ctx.session_mem.get('email'))
                    _have_phone = bool(ctx.session_mem.get('phone'))

                    if _have_email or _have_phone:
                        # Contact info already in session — confirm and close
                        handoff_payload = self._build_handoff_payload(
                            conversation_history, ctx.session_mem,
                            ctx.clean_message, session_id, 'accepted'
                        )
                        ctx.session_mem['handoff_offered'] = False
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                        _bg_submit(
                            self._persist_lead_and_notify,
                            client_id, handoff_payload, vertical
                        )
                        contact = ctx.session_mem.get('email') or ctx.session_mem.get('phone')
                        return PipelineResult(
                            response=(
                                f"Perfect — someone from the team will be in touch "
                                f"at {contact} shortly."
                            ),
                            method='handoff_accepted',
                            confidence=1.0,
                            handoff=handoff_payload,
                        ).to_dict()

                    # No email yet — pivot to capture before confirming
                    ctx.session_mem['email_capture_pending'] = True
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult(
                        response=self._build_email_request(vertical),
                        method='email_capture_prompt',
                        confidence=1.0,
                    ).to_dict()

                if any(k in msg_lower for k in [
                    'no', 'nope', 'not now', 'no thanks', "don't", "i'm fine", 'im fine'
                ]):
                    ctx.session_mem['handoff_offered'] = False
                    _bg_submit(
                        persist_session, client_id, session_id, ctx.session_mem
                    )
                    return PipelineResult.declined_handoff().to_dict()

            # ── Escalation check ──────────────────────────────────────
            escalation_text = check_escalation(
                ctx.clean_message, ctx.session_mem, vertical
            )
            if escalation_text:
                handoff_payload = self._build_handoff_payload(
                    conversation_history, ctx.session_mem,
                    ctx.clean_message, session_id, 'escalation'
                )
                ctx.session_mem['handoff_offered'] = True
                _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                # FIX: was `if ctx.session_mem.get('email')` only — phone-only
                # leads never triggered persistence even though
                # _persist_lead_and_notify now supports them.
                if ctx.session_mem.get('email') or ctx.session_mem.get('phone'):
                    _bg_submit(
                        self._persist_lead_and_notify,
                        client_id, handoff_payload, vertical
                    )
                return PipelineResult(
                    response=escalation_text,
                    method='escalation',
                    confidence=1.0,
                    handoff=handoff_payload,
                ).to_dict()

            # ── Response cache read (deferred — see comment above) ─────
            # Now that email-capture / handoff / escalation have all had
            # their chance to intercept this turn, it's safe to consult the
            # cache. Still not consumed here though — see "Query resolution"
            # further down, where it's used to skip retrieval + generation
            # while still letting intent detection and lead capture run
            # fresh for THIS turn (fixes the FAQ-cache PII leak below too).
            #
            # FIX: the cache key is exact-message-text based and doesn't
            # capture conversation history at all. A context-dependent
            # follow-up ("what about the second option") means something
            # different in every conversation it appears in — if two
            # different users' conversations both happen to produce that
            # exact literal text, one could get served the other's cached
            # answer even though the underlying question was completely
            # different. is_followup() already exists as exactly the
            # signal needed to detect this, so skip the cache entirely
            # (read and, at the write site below, write) for messages it
            # flags — safer to regenerate than to risk serving an answer
            # to the wrong "it"/"that"/"the second one".
            _is_followup_msg = is_followup(ctx.clean_message)
            _top_cached = (
                None if _is_followup_msg
                else self._read_resp_cache(ctx.resp_cache_key)
            )

            # ── Load poor KB IDs ──────────────────────────────────────
            try:
                poor = get_poor_answers(client_id or '', limit=100)
                ctx.poor_kb_ids = {
                    str(p.get('kb_id', '')) for p in poor if p.get('kb_id')
                }
            except Exception:
                ctx.poor_kb_ids = set()

            # ── Pending write-tool confirmation ────────────────────────
            # Checked BEFORE intent detection: a bare "yes"/"no", or a
            # message that's just an email/slot selection, won't match any
            # keyword tier and would otherwise fall through to a wasted
            # Tier-3 Gemini call or a completely unrelated FAQ answer.
            # Two states, tracked via pending_tool_action['status']:
            #   'awaiting_info'         — still missing required args
            #   'awaiting_confirmation' — ready, waiting on yes/no
            _pending = ctx.session_mem.get('pending_tool_action')
            if _pending and (time.time() - _pending.get('created_at', 0)) < PENDING_TOOL_ACTION_TTL_SECONDS:
                _tool_name = _pending['tool']

                if _pending.get('status') == 'awaiting_info':
                    # FIX: this state didn't used to exist at all — a write
                    # tool missing a required arg (e.g. cancel_order without
                    # a known email) just asked the question and threw away
                    # everything already extracted (the order_id). The next
                    # message ("it's jane@x.com") contains no tool keywords,
                    # so intent detection wouldn't even recognise it as
                    # continuing the same request — the whole attempt
                    # silently died. Now the partial args persist and this
                    # turn's message is used to fill in what's missing.
                    _email_m = _EMAIL_RE.search(ctx.clean_message)
                    if _email_m:
                        ctx.session_mem['email'] = _email_m.group(0)
                    _phone_m = _PHONE_RE.search(ctx.clean_message)
                    if _phone_m:
                        ctx.session_mem['phone'] = _phone_m.group(0).strip()

                    _merged_args = {
                        **_pending['args'],
                        **extract_tool_args(_tool_name, ctx.clean_message, ctx.session_mem),
                    }
                    if _tool_name == 'book_appointment' and not _merged_args.get('slot_id'):
                        _slot_id = resolve_slot_selection(
                            ctx.clean_message, ctx.session_mem.get('available_slots', [])
                        )
                        if _slot_id:
                            _merged_args['slot_id'] = _slot_id

                    _still_missing = missing_required_args(_tool_name, _merged_args)
                    if not _still_missing:
                        ctx.session_mem['pending_tool_action'] = {
                            'tool': _tool_name, 'args': _merged_args,
                            'created_at': time.time(), 'status': 'awaiting_confirmation',
                        }
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                        return PipelineResult(
                            response=build_confirmation_prompt(_tool_name, _merged_args),
                            method='tool_confirmation_requested',
                            confidence=0.9,
                        ).to_dict()

                    _retries = _pending.get('retries', 0) + 1
                    if _retries > 2:
                        # Gave it 2 tries — don't keep pestering. Drop the
                        # pending action and let this message fall through
                        # to normal handling; they may have moved on.
                        ctx.session_mem['pending_tool_action'] = None
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    else:
                        ctx.session_mem['pending_tool_action'] = {
                            'tool': _tool_name, 'args': _merged_args,
                            'created_at': _pending['created_at'],
                            'status': 'awaiting_info', 'retries': _retries,
                        }
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                        return PipelineResult(
                            response=build_missing_args_prompt(_tool_name, _still_missing),
                            method='tool_missing_info',
                            confidence=0.9,
                        ).to_dict()

                else:  # 'awaiting_confirmation'
                    _confirmed = detect_confirmation(ctx.clean_message)
                    if _confirmed is True:
                        ctx.session_mem['pending_tool_action'] = None
                        tool_result = dispatch_tool(
                            _tool_name, ctx.clean_message, client_id, ctx.session_mem,
                            override_args=_pending['args'], session_id=session_id,
                        )
                        # FIX: cancel_order/book_appointment only update
                        # Lumvi's own synced tables — there's no outbound
                        # call to the client's real Shopify/Acuity/Calendly/
                        # etc. account, so nothing happens there unless a
                        # human actually does it. Fire an internal ticket so
                        # staff see the request and can execute it for real;
                        # this is what makes "our team will confirm this"
                        # (the wording tools.py now uses) actually true
                        # rather than just a nicer-sounding false promise.
                        if _tool_name in ('cancel_order', 'book_appointment') and tool_result.get('success'):
                            _detail = ', '.join(f"{k}={v}" for k, v in _pending['args'].items())
                            _reason = (
                                f"[Action needed] {_tool_name} requested via chatbot "
                                f"({_detail}). Only recorded in Lumvi — please action on "
                                f"the real platform and confirm with the customer."
                            )
                            _bg_submit(
                                dispatch_tool, 'escalate_to_human', _reason, client_id,
                                ctx.session_mem, session_id=session_id,
                            )
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                        return PipelineResult(
                            response=tool_result.get('response', make_fallback(vertical)),
                            method='tool_confirmed',
                            confidence=0.9 if tool_result.get('success') else 0.0,
                            action={'tool': _tool_name, **tool_result},
                        ).to_dict()
                    elif _confirmed is False:
                        ctx.session_mem['pending_tool_action'] = None
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                        return PipelineResult(
                            response=(
                                "No problem — I haven't made any changes. "
                                "Is there anything else I can help with?"
                            ),
                            method='tool_confirmation_declined',
                            confidence=1.0,
                        ).to_dict()
                    # else: not a yes/no reply — leave pending_tool_action in
                    # place (still within TTL) and fall through to normal
                    # handling; this turn might be an unrelated new message.
            elif _pending:
                # Expired — clear it so a stray "yes" long afterward can't
                # trigger a stale action.
                ctx.session_mem['pending_tool_action'] = None

            # ── Intent detection ──────────────────────────────────────
            # FIX: was hardcoded model=None, skip_gemini=True — meaning
            # Tier 3 (the Gemini fallback for messages no keyword list
            # catches) never ran in production, full stop. Any lead/sales
            # signal that didn't match a hardcoded keyword was silently
            # lost with zero AI fallback. Now passes the real model and
            # only skips when Gemini genuinely isn't configured. Tier 3
            # only calls Gemini when the free keyword tiers left is_lead
            # or is_sales unresolved (see intent.py), so this stays rare.
            intent = detect_intent(
                ctx.clean_message, vertical, lead_triggers,
                model=self.model,
                skip_gemini=not (self.enabled and self.model is not None),
            )
            # Tier 3 spent a real Gemini call this turn — charge it against
            # the same shared budget as query-rewrite/cross-encoder rerank
            # so a single turn can't rack up 3 Gemini calls (classification
            # + rewrite/cross-encoder + RAG generation). See call1_used
            # usage further down and the matching guard added to the
            # query-rewrite trigger below.
            if intent.get('gemini_used'):
                ctx.call1_used = True

            # ── Stage → is_lead coercion ──────────────────────────────
            # STAGE_SIGNALS already scanned this message above. Promote
            # browsing/evaluating → is_lead so prospects found via stage
            # signals get the same email-nudge flow as those caught by
            # PROSPECT_INFO_KEYWORDS or GLOBAL_PRICING_KW.
            if not intent.get('is_lead'):
                _stage = ctx.session_mem.get('purchase_stage')
                if _stage in ('browsing', 'evaluating'):
                    intent['is_lead'] = True

            # ── Simple intent exit (no retrieval needed) ──────────────
            if intent['intent'] == 'goodbye':
                return PipelineResult.goodbye().to_dict()
            if intent['intent'] == 'gratitude':
                return PipelineResult.gratitude().to_dict()

            # ── Action exit (demo / meeting / contact CTA) ────────────
            if intent['intent'] == 'action' and intent.get('action'):
                action_data = handle_detected_action(intent['action'], vertical)
                lead_info = self._extract_lead_info(
                    ctx.clean_message, ctx.session_mem, vertical, lead_triggers
                )
                ctx.session_mem['handoff_offered'] = True
                _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                return PipelineResult(
                    response=action_data['response'],
                    method='action',
                    confidence=1.0,
                    is_lead=True,
                    lead_metadata=lead_info,
                    action=action_data,
                ).to_dict()

            # ── Tool dispatch exit ────────────────────────────────────
            # FIX: search_products had no plan gate at all — PLAN_LIMITS
            # ['product_recommendations'] is False on Free/Starter but was
            # never actually checked anywhere in the dispatch path, so
            # every plan got full AI product search regardless. Gated here
            # rather than inside dispatch_tool/tools.py: on a blocked plan
            # this falls through to normal FAQ/RAG handling below (as if no
            # tool had been detected), rather than dispatching and then
            # returning an error — the shopper still gets a helpful answer,
            # and no "upgrade your plan" messaging leaks to them (that's
            # the merchant's billing relationship, not theirs to see).
            if (intent['intent'] == 'tool' and intent.get('tool')
                    and not (intent['tool'] == 'search_products'
                             and not ctx.product_recommendations_enabled)):
                tool_name = intent['tool']

                if is_write_tool(tool_name):
                    # DESIGN: cancellation/refunds and appointment booking
                    # are both redirected to the client's real platform
                    # rather than executed by the chatbot, when a real
                    # connection is configured — cancel_order to their
                    # order-management page, book_appointment to their
                    # real Acuity/Calendly/Square booking page. Order
                    # *lookup* (read-only) is the one thing this stays
                    # fully chatbot-handled for, via commerce_adapters.py's
                    # live Shopify/WooCommerce read, with Lumvi's
                    # webhook-synced table as fallback — see tools.py.
                    #
                    # Checked here, before any propose/confirm machinery
                    # runs: when a redirect applies, the outcome is just a
                    # link, not a mutation — there's nothing to confirm,
                    # and for book_appointment specifically, skipping this
                    # check would mean session_mem['available_slots'] is
                    # never populated (check_availability redirects
                    # instead of listing slots), so slot_id could never be
                    # resolved and the bot would loop asking "which slot?"
                    # forever with no way to answer.
                    if tool_name == 'book_appointment':
                        _redirect = booking_redirect_message(client_id)
                        if _redirect:
                            return PipelineResult(
                                response=_redirect,
                                method='booking_redirect',
                                confidence=0.9,
                            ).to_dict()
                    elif tool_name == 'cancel_order':
                        _redirect = order_cancellation_redirect_message(client_id)
                        if _redirect:
                            return PipelineResult(
                                response=_redirect,
                                method='cancellation_redirect',
                                confidence=0.9,
                            ).to_dict()

                    # FIX: cancel_order/book_appointment mutate real state
                    # (an actual order, an actual calendar slot) and must
                    # never fire on a single keyword match. Propose first —
                    # extract what we can, ask for anything still missing,
                    # and stash the args for confirmation on a later turn
                    # (see the pending-confirmation check above). Only
                    # dispatches once the user replies "yes".
                    _args = extract_tool_args(tool_name, ctx.clean_message, ctx.session_mem)

                    # FIX: slot_id has no way to come from free text on its
                    # own — it only means anything relative to a slot list
                    # we already showed via check_availability. Try to
                    # resolve it against whatever's remembered in session.
                    if tool_name == 'book_appointment' and not _args.get('slot_id'):
                        _slot_id = resolve_slot_selection(
                            ctx.clean_message, ctx.session_mem.get('available_slots', [])
                        )
                        if _slot_id:
                            _args['slot_id'] = _slot_id

                    _missing = missing_required_args(tool_name, _args)
                    if _missing:
                        ctx.session_mem['pending_tool_action'] = {
                            'tool':       tool_name,
                            'args':       _args,
                            'created_at': time.time(),
                            'status':     'awaiting_info',
                            'retries':    0,
                        }
                        response_text = build_missing_args_prompt(tool_name, _missing)
                        method = 'tool_missing_info'
                    else:
                        ctx.session_mem['pending_tool_action'] = {
                            'tool':       tool_name,
                            'args':       _args,
                            'created_at': time.time(),
                            'status':     'awaiting_confirmation',
                        }
                        response_text = build_confirmation_prompt(tool_name, _args)
                        method = 'tool_confirmation_requested'
                    _bg_submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult(
                        response=response_text,
                        method=method,
                        confidence=0.9,
                    ).to_dict()

                # Read-only / reversible tools — safe to dispatch immediately.
                tool_result = dispatch_tool(
                    tool_name, ctx.clean_message, client_id, ctx.session_mem,
                    session_id=session_id,
                )

                # FIX: check_availability's slots were shown to the user as
                # text but never remembered anywhere — a follow-up booking
                # attempt had no slot list to resolve "the 2pm one" against.
                # Store it (numbered in the same order shown) so
                # resolve_slot_selection above can use it on the next turn.
                if tool_name == 'check_availability' and tool_result.get('success'):
                    _slots = tool_result.get('data', {}).get('slots', [])
                    if _slots:
                        ctx.session_mem['available_slots'] = _slots
                        _bg_submit(persist_session, client_id, session_id, ctx.session_mem)

                return PipelineResult(
                    response=tool_result.get('response', make_fallback(vertical)),
                    method='tool',
                    confidence=0.9 if tool_result.get('success') else 0.0,
                    action={'tool': tool_name, **tool_result},
                ).to_dict()

            # ── Confidence thresholds (sales queries get lower floor) ──
            ctx.is_sales_query = intent.get('is_sales', False)
            if ctx.is_sales_query:
                ctx.vector_threshold -= 0.05
                ctx.confidence_high  -= 0.05
                ctx.confidence_medium -= 0.05

            # ── Lead detection ────────────────────────────────────────
            is_lead      = intent.get('is_lead', False)
            lead_metadata: Optional[Dict] = None
            if is_lead:
                lead_info = self._extract_lead_info(
                    ctx.clean_message, ctx.session_mem, vertical, lead_triggers
                )
                if lead_info:
                    ctx.session_mem.update({
                        k: v for k, v in lead_info.items()
                        if v and k in ('name', 'email', 'phone')
                    })
                    lead_metadata = lead_info

            # ── Pronoun short-circuit → clarification ─────────────────
            words = ctx.clean_message.lower().split()
            if len(words) <= 3 and all(w in BARE_PRONOUNS for w in words if len(w) > 1):
                category = last_response_category(conversation_history)
                clarification = (
                    f"Could you tell me a bit more about what you'd like to know "
                    f"about {category}?" if category else
                    "Could you tell me a little more about what you're looking for?"
                )
                return PipelineResult(
                    response=clarification,
                    method='clarification',
                    confidence=1.0,
                ).to_dict()

            # ── Query resolution + retrieval + generation ──────────────
            # FIX: a top-level cache hit no longer returns immediately (see
            # the deferred read above). Retrieval/generation is skipped
            # here instead — preserving the speed benefit — but the result
            # still flows through the normal post-processing below
            # (guardrails, lead nudge, session persistence, handoff, KB gap
            # recording) using THIS turn's freshly-computed is_lead /
            # lead_metadata, not whatever was true when the cached answer
            # was first generated. Combined with stripping is_lead /
            # lead_metadata out of what gets cached (see cache-write
            # further down), a cache hit can no longer replay one
            # customer's captured contact info into another customer's
            # conversation, and no longer silently drops this turn's own
            # lead signal just because the answer text was reusable.
            #
            # _faq_cache_key/_faq_cached are initialized here — outside the
            # if/else — rather than inside the "else" branch below. They
            # were previously first assigned only inside that branch, so a
            # top-level cache hit (which takes the "if" branch and skips
            # "else" entirely) would leave them undefined, and the
            # cache-write section further down would raise a NameError on
            # every single cache-hit turn.
            _faq_cache_key: Optional[str] = None
            _faq_cached: Optional[Dict] = None

            if _top_cached and _top_cached.get('response'):
                final      = _top_cached['response']
                confidence = _top_cached.get('confidence', 0.75)
                method     = _top_cached.get('method', 'cache')
                logger.debug(f"[Pipeline] cache hit trace={ctx.trace_id}")
            else:
                ctx.search_query = resolve_query(ctx.clean_message, conversation_history)

                # ── Multi-intent decomposition ──────────────────────────
                sub_queries = decompose_intents(ctx.search_query)

                # ── Helper: run embedding search across sub-queries ─────
                def _do_embedding_search(query: str, sq_list: List[str]) -> tuple:
                    if len(sq_list) > 1:
                        merged_c: List[Dict] = []
                        merged_s: List[float] = []
                        seen: set = set()
                        for sq in sq_list[:3]:
                            cands, scores = embedding_search(sq, faqs, client_id, ctx.poor_kb_ids)
                            for c, s in zip(cands, scores):
                                cid = str(c.get('kb_id', c.get('id', id(c))))
                                if cid not in seen:
                                    merged_c.append(c)
                                    merged_s.append(s)
                                    seen.add(cid)
                        # FIX: results were appended sub-query-by-sub-query
                        # (all of sub_queries[0]'s hits, then sub_queries[1]'s
                        # new ones, ...), so merged_s[0] was only ever
                        # sub_queries[0]'s top score — never the true best
                        # score across the whole compound question. Every
                        # downstream consumer assumes index 0 is the best
                        # match: ctx.top_cosine (confidence gating, and the
                        # query-rewrite/cross-encoder trigger conditions) and
                        # hybrid_rerank's RRF vector-rank component both read
                        # this list assuming it's sorted. Sort once, by
                        # score, before returning.
                        if merged_s:
                            order = sorted(
                                range(len(merged_s)),
                                key=lambda i: merged_s[i],
                                reverse=True,
                            )
                            merged_c = [merged_c[i] for i in order]
                            merged_s = [merged_s[i] for i in order]
                        return merged_c, merged_s
                    return embedding_search(query, faqs, client_id, ctx.poor_kb_ids)

                # ── Preflight embedding search ───────────────────────────
                ctx.candidates, ctx.vector_scores = _do_embedding_search(
                    ctx.search_query, sub_queries
                )
                ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

                # ── BM25-only fallback (embeddings unavailable) ──────────
                # FIX: embedding_search() returns [], [] both when nothing
                # matches well AND when the embedding provider itself is
                # down / every FAQ lacks a usable embedding. Previously
                # both cases fell straight through to the generic
                # dynamic_fallback for every single query — a full outage
                # of the smart-answer path triggered by what might be a
                # transient external dependency issue. If the FAQ corpus
                # itself is non-empty but preflight search found nothing,
                # fall back to pure keyword search instead of going dark.
                _bm25_fallback_active = False
                if not ctx.candidates and faqs:
                    _bm25_c, _bm25_s = bm25_only_search(
                        ctx.search_query, faqs, ctx.poor_kb_ids
                    )
                    if _bm25_c:
                        ctx.candidates        = _bm25_c
                        ctx.vector_scores     = _bm25_s
                        _bm25_fallback_active = True
                        logger.warning(
                            f"[Pipeline] embeddings unavailable — BM25-only "
                            f"fallback active trace={ctx.trace_id}"
                        )

                # ── Query rewriting (Call 1) ─────────────────────────────
                # FIX: skipped entirely in BM25-fallback mode — its trigger
                # condition (ctx.top_cosine < ctx.confidence_high) compares
                # a BM25 score against a cosine-scale threshold when
                # embeddings are down, and rewriting via Gemini won't
                # recover an embedding call that isn't happening anyway.
                if (not _bm25_fallback_active
                        and self.enabled and self.model
                        and not ctx.call1_used
                        and len(ctx.search_query.split()) >= 4
                        and ctx.top_cosine < ctx.confidence_high):
                    rewritten = rewrite_query(
                        ctx.search_query, vertical, conversation_history, self.model
                    )
                    if rewritten != ctx.search_query:
                        ctx.search_query = rewritten
                        ctx.call1_used   = True
                        sub_queries      = decompose_intents(rewritten)
                        ctx.candidates, ctx.vector_scores = _do_embedding_search(
                            ctx.search_query, sub_queries
                        )
                        ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

                # ── Hybrid rerank (RRF) ──────────────────────────────────
                # FIX: now passes poor_kb_ids through — see hybrid_rerank()
                # docstring for why the penalty needs to reach the BM25 side
                # too, not just the vector-search stage. Skipped entirely in
                # BM25-fallback mode: vector_scores already holds raw BM25
                # scores there (no real vector signal to fuse), so calling
                # hybrid_rerank would just re-score BM25 against itself.
                if _bm25_fallback_active:
                    ctx.hybrid_ranked, ctx.hybrid_scores = ctx.candidates, ctx.vector_scores
                else:
                    ctx.hybrid_ranked, ctx.hybrid_scores = hybrid_rerank(
                        ctx.search_query, ctx.candidates, ctx.vector_scores, faqs,
                        poor_kb_ids=ctx.poor_kb_ids,
                    )
                ctx.top_hybrid = ctx.hybrid_scores[0] if ctx.hybrid_scores else 0.0

                # ── Cross-encoder rerank ─────────────────────────────────
                # FIX: also skipped in BM25-fallback mode — ctx.top_cosine
                # is a BM25 score there, not a cosine similarity, so
                # comparing it against the 0.35 cosine-scale trigger
                # wouldn't mean what it's supposed to mean.
                if (not _bm25_fallback_active
                        and self.enabled and self.model
                        and not ctx.call1_used
                        and ctx.top_cosine < 0.35
                        and len(ctx.hybrid_ranked) >= 2):
                    ctx.hybrid_ranked = cross_encoder_rerank(
                        ctx.search_query, ctx.hybrid_ranked, self.model
                    )
                    ctx.call1_used = True

                # FIX: cross-encoder rerank is Gemini judging text relevance —
                # it has no idea a candidate was ever flagged as a poor
                # answer, so it can promote one straight back to #1. This is
                # the last reranking stage before generation, so it doubles
                # as a final safety net regardless of which reranking paths
                # actually ran. Doesn't remove the poor entry (it may still
                # be the best available option) — just won't let it win the
                # top slot over a non-poor alternative if one exists.
                if ctx.poor_kb_ids and ctx.hybrid_ranked:
                    _top_c  = ctx.hybrid_ranked[0]
                    _top_id = str(_top_c.get('kb_id', _top_c.get('id', '')))
                    if _top_id in ctx.poor_kb_ids:
                        _better_idx = next(
                            (i for i, c in enumerate(ctx.hybrid_ranked[1:], start=1)
                             if str(c.get('kb_id', c.get('id', ''))) not in ctx.poor_kb_ids),
                            None
                        )
                        if _better_idx is not None:
                            ctx.hybrid_ranked.insert(0, ctx.hybrid_ranked.pop(_better_idx))

                # ── RAG qualification gate ───────────────────────────────
                # FIX: previously gated on ctx.top_cosine alone — the
                # *preflight* cosine score computed before hybrid (BM25+RRF)
                # rerank ran. That meant reranking could never rescue a
                # query: if the raw vector search's top hit scored below
                # threshold, generation was skipped even when BM25/keyword
                # rerank surfaced a strong match afterward. We now also
                # check the cosine score of whichever candidate ended up on
                # top *after* rerank, and qualify on whichever is higher.
                _vec_score_by_id = {
                    str(c.get('kb_id', c.get('id', ''))): s
                    for c, s in zip(ctx.candidates, ctx.vector_scores)
                }
                _top_reranked_cosine = 0.0
                if ctx.hybrid_ranked:
                    _top_id = str(
                        ctx.hybrid_ranked[0].get('kb_id', ctx.hybrid_ranked[0].get('id', ''))
                    )
                    _top_reranked_cosine = _vec_score_by_id.get(_top_id, 0.0)

                if _bm25_fallback_active:
                    # FIX: BM25 scores are a different scale than cosine
                    # similarity and must never be compared against
                    # vector_threshold directly — that threshold is
                    # calibrated for cosine. Uses its own, intentionally
                    # permissive bar (_BM25_FALLBACK_MIN_SCORE) since this
                    # path's whole purpose is avoiding a full outage, not
                    # matching normal RAG precision.
                    ctx.rag_qualified = (
                        bool(ctx.hybrid_ranked)
                        and ctx.vector_scores[0] > _BM25_FALLBACK_MIN_SCORE
                    )
                    # BM25 scores are an unbounded, different scale from
                    # cosine similarity — never let this degraded path's
                    # generation confidence exceed a conservative cap,
                    # regardless of how high the raw BM25 score runs. This
                    # path exists to avoid a full outage, not to match
                    # normal RAG precision, and confidence should reflect
                    # that.
                    _gate_score = (
                        min(0.5, ctx.vector_scores[0] / 4.0) if ctx.vector_scores else 0.0
                    )
                else:
                    ctx.rag_qualified = (
                        max(ctx.top_cosine, _top_reranked_cosine) >= ctx.vector_threshold
                    )
                    # This is also the score handed to rag_generate_and_polish
                    # below so confidence reflects the actual retrieval match
                    # quality instead of a flat heuristic (see generation.py).
                    _gate_score = max(ctx.top_cosine, _top_reranked_cosine)

                # ── Topic mismatch gate ───────────────────────────────────
                if ctx.rag_qualified and ctx.hybrid_ranked:
                    top_faq = ctx.hybrid_ranked[0]
                    # FIX: was a dynamic __import__('pipeline.stages.math_helpers',
                    # fromlist=['topic_overlap']) done inline on every qualified
                    # turn — the only place in this file NOT using a clean
                    # top-level import, for no real benefit (the module is a
                    # stable leaf with no circular-import risk). Now uses the
                    # top-level import like everything else.
                    # FIX: was 0.05 — close to a no-op, since almost any two
                    # related sentences clear 5% word overlap. 0.15 actually
                    # catches genuine topic mismatches without being so
                    # strict it flags legitimate paraphrased matches.
                    #
                    # FIX: was topic_overlap(ctx.clean_message, ...) — the
                    # raw, un-resolved message. For short follow-ups
                    # ("what about the Enterprise plan?"), resolve_query()
                    # already expanded ctx.search_query with the topic from
                    # the previous turn specifically so retrieval has
                    # something concrete to match against. Checking this
                    # gate against the un-resolved clean_message threw that
                    # context away and could reject a genuinely correct
                    # follow-up match for sharing almost no literal words
                    # with the FAQ question.
                    if topic_overlap(ctx.search_query, top_faq.get('question', '')) < 0.15:
                        ctx.rag_qualified = False
                        logger.debug(
                            f"[Pipeline] topic mismatch gate fired trace={ctx.trace_id}"
                        )

                # ── FAQ-keyed semantic cache ──────────────────────────────
                # FIX: previously `return _faq_cached` here — an early
                # return that (a) discarded this turn's freshly-extracted
                # is_lead/lead_metadata entirely, and (b) replayed whatever
                # is_lead/lead_metadata was cached from a PREVIOUS, different
                # customer's turn that happened to match the same FAQ — a
                # real cross-customer PII leak. Now only the safe, universal
                # answer text is taken from the cache; everything else
                # (lead nudge, persistence, handoff) continues normally
                # below using this turn's real data. Also skipped entirely
                # in BM25-fallback mode — a keyword-matched top pick is less
                # trustworthy than a semantic one, and caching it under the
                # FAQ key would keep serving that weaker match even after
                # embeddings come back online.
                if ctx.rag_qualified and ctx.hybrid_ranked and not _bm25_fallback_active:
                    _top_faq_id = str(
                        ctx.hybrid_ranked[0].get('kb_id',
                        ctx.hybrid_ranked[0].get('id', ''))
                    )
                    if _top_faq_id:
                        _faq_cache_key = self._cache_key(
                            _top_faq_id, client_id or '', vertical
                        )
                        # FIX: this cache is keyed on (top_faq_id, client_id,
                        # vertical) alone — not on the query text. Without a
                        # similarity check, ANY future message that happens
                        # to rank this same FAQ as its top hit would replay
                        # the exact same generated answer verbatim, even if
                        # it's asking about a genuinely different angle of
                        # that FAQ (e.g. "what's the price" vs "does that
                        # price include support"). The cached entry now also
                        # stores the query it was generated for, and a hit
                        # is only trusted when the current query still has
                        # meaningful topical overlap with it — otherwise we
                        # fall through and regenerate fresh.
                        _candidate_faq_cached = self._read_resp_cache(_faq_cache_key)
                        if _candidate_faq_cached and _candidate_faq_cached.get('response'):
                            _cached_query = _candidate_faq_cached.get('query', '')
                            if not _cached_query or topic_overlap(
                                ctx.search_query, _cached_query
                            ) >= 0.3:
                                _faq_cached = _candidate_faq_cached
                            else:
                                logger.debug(
                                    f"[Pipeline] FAQ-cache skipped — query "
                                    f"drift trace={ctx.trace_id}"
                                )

                # ── Generation ────────────────────────────────────────────
                if _faq_cached and _faq_cached.get('response'):
                    final      = _faq_cached['response']
                    confidence = _faq_cached.get('confidence', 0.75)
                    method     = _faq_cached.get('method', 'rag_cached')
                    logger.debug(f"[Pipeline] FAQ-keyed cache hit trace={ctx.trace_id}")
                elif ctx.rag_qualified and self.enabled and self.model:
                    # FIX: context building (+ its DB summary lookup) used to
                    # run unconditionally for every turn, even ones that
                    # would end up in dynamic_fallback — which doesn't even
                    # take context_str as a parameter, so that work was
                    # wasted on every fallback turn. Now only built right
                    # before it's actually consumed, i.e. right here.
                    # session_id is passed through so the stored-summary
                    # lookup stays scoped per-session (see generation.py).
                    _bg_submit(
                        maybe_summarise, client_id, conversation_history, self.model,
                        session_id=session_id,
                    )
                    ctx.context_str = build_context(
                        conversation_history, client_id, ctx.clean_message,
                        session_id=session_id,
                    )
                    final, confidence, method = rag_generate_and_polish(
                        ctx.search_query,
                        ctx.context_str,
                        ctx.hybrid_ranked,
                        vertical,
                        ctx.session_mem,
                        ctx.is_sales_query,
                        self.model,
                        retrieval_score=_gate_score,
                    )
                elif self.enabled and self.model:
                    final = dynamic_fallback(
                        ctx.search_query, vertical, ctx.session_mem,
                        self.model, self._model_name,
                    )
                    confidence = 0.0
                    method     = 'dynamic_fallback_idk'
                else:
                    final      = dynamic_fallback(
                        ctx.search_query, vertical, ctx.session_mem, None
                    )
                    confidence = 0.0
                    method     = 'static_fallback'

            # ── CLARIFY handling ──────────────────────────────────────
            clarify_q = parse_clarify_response(final)
            if clarify_q:
                return PipelineResult(
                    response=clarify_q,
                    method='clarification',
                    confidence=1.0,
                ).to_dict()

            # ── Guardrails ────────────────────────────────────────────
            final = guardrails(final, ctx.hybrid_ranked)

            # ── Cache write ────────────────────────────────────────────
            # FIX: previously bundled is_lead/lead_metadata (name, email,
            # phone) into the cached object at BOTH cache keys. Since
            # neither cache key is scoped by session, a later cache hit —
            # especially the FAQ-keyed one, which matches on semantic
            # similarity rather than exact text — would replay one
            # customer's captured contact info onto a completely different
            # customer's turn. Only the safe, universal part (the answer
            # text itself) is cacheable; is_lead/lead_metadata must always
            # be computed fresh per turn, which they now are (see the
            # retrieval/generation restructure above).
            _result_to_cache = {
                'response':   final,
                'method':     method,
                'confidence': confidence,
                # FIX: stored so a future FAQ-keyed cache read (see above)
                # can check the new query still overlaps topically with
                # this one before trusting the cached answer.
                'query':      ctx.search_query[:200],
            }

            # FIX: don't write the raw-message cache entry for follow-ups
            # either — see the read-side comment above. The FAQ-keyed write
            # is unaffected: it's keyed by the matched FAQ's own stable id,
            # not by this message's ambiguous literal text, so it's safe to
            # keep caching there regardless.
            if confidence >= ctx.confidence_high and ctx.resp_cache_key:
                if not _is_followup_msg:
                    _bg_submit(self._write_resp_cache, ctx.resp_cache_key, _result_to_cache)
                if _faq_cache_key:
                    _bg_submit(self._write_resp_cache, _faq_cache_key, _result_to_cache)
            elif method == 'dynamic_fallback_idk' and ctx.resp_cache_key and not _is_followup_msg:
                _bg_submit(
                    self._write_resp_cache_ttl,
                    ctx.resp_cache_key,
                    _result_to_cache,
                    _REDIS_IDK_TTL_SEC,
                )

            # ── KB gap recording ──────────────────────────────────────
            if method in IDK_METHODS_ALL:
                _bg_submit(
                    record_kb_gap, client_id or '', ctx.clean_message,
                    method, confidence, session_id
                )
                ctx.session_mem['handoff_offered'] = True

            # ── Lead nudge ────────────────────────────────────────────
            # FIX: previously fired regardless of method, so an IDK/fallback
            # response (which already ends with its own handoff CTA, e.g.
            # "want me to connect you?") would get a second, differently
            # worded ask appended ("what's the best email..."). Two CTAs in
            # one bubble reads as confused/scripted. IDK methods already
            # request a handoff via the fallback text itself, so we skip the
            # nudge there and let the existing CTA stand alone.
            trigger_lead_collection = False
            _have_email = bool(
                (lead_metadata or {}).get('email') or ctx.session_mem.get('email')
            )
            if is_lead and not _have_email and method not in IDK_METHODS_ALL:
                nudge = self._build_lead_nudge(
                    lead_info=ctx.session_mem,
                    vertical=vertical,
                    purchase_stage=ctx.session_mem.get('purchase_stage'),
                    is_sales=ctx.is_sales_query,
                    lead_q3=lead_q3,
                )
                # FIX: was appended straight onto `final` after guardrails()
                # had already run on the generated answer — meaning the
                # nudge itself (which can be a per-client custom question,
                # lead_q3, configured outside this code) never passed the
                # same legal/medical/financial-advice safety check. Any
                # text that ends up in the customer-facing message should
                # get the same check, regardless of where it came from.
                nudge = guardrails(nudge, ctx.hybrid_ranked)
                final += '\n\n' + nudge
                trigger_lead_collection = True

            # ── Session persistence ───────────────────────────────────
            if client_id and session_id:
                _bg_submit(persist_session, client_id, session_id, ctx.session_mem)

            # ── Handoff payload for IDK paths ─────────────────────────
            handoff = None
            if method in IDK_METHODS_ALL or ctx.session_mem.get('handoff_offered'):
                handoff = self._build_handoff_payload(
                    conversation_history, ctx.session_mem,
                    ctx.clean_message, session_id, method
                )
                # Persist lead if we already have contact info (e.g. captured
                # in a prior turn and IDK fires later in the conversation).
                # FIX: was email-only — phone-only leads never persisted here.
                if ctx.session_mem.get('email') or ctx.session_mem.get('phone'):
                    _bg_submit(
                        self._persist_lead_and_notify,
                        client_id, handoff, vertical
                    )

            logger.info(
                f"[Pipeline] done trace={ctx.trace_id} method={method} "
                f"conf={confidence:.2f} elapsed={ctx.elapsed_ms:.0f}ms "
                f"client={client_id}"
            )

            return PipelineResult(
                response=final,
                method=method,
                confidence=round(confidence, 4),
                is_lead=is_lead,
                lead_metadata=lead_metadata,
                handoff=handoff,
                trigger_lead_collection=trigger_lead_collection,
            ).to_dict()

        except Exception as e:
            log_crash(logger, 'Pipeline/Fatal', e,
                      client_id=client_id,
                      trace_id=ctx.trace_id,
                      msg_preview=user_message[:80])
            return PipelineResult.fatal_error().to_dict()

    # ── KB management methods ─────────────────────────────────────────────────

    def _split_content(self, text: str, max_chars: int = 1200) -> List[str]:
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        chunks, current = [], ''
        for sent in sentences:
            if len(current) + len(sent) + 1 <= max_chars:
                current = (current + ' ' + sent).strip()
            else:
                if current:
                    chunks.append(current)
                current = sent
        if current:
            chunks.append(current)
        return chunks or [text[:max_chars]]

    def _extract_tags(self, text: str) -> List[str]:
        words = re.findall(r'\b[a-zA-Z]{4,}\b', text.lower())
        from collections import Counter
        freq = Counter(words)
        return [w for w, _ in freq.most_common(8) if w not in {
            'that', 'this', 'with', 'from', 'have', 'will', 'your',
            'what', 'when', 'where', 'also', 'more', 'some', 'been',
        }]

    def _ai_enrich(self, question: str, answer: str, vertical: str) -> Dict:
        if not self.enabled or not self.model:
            return {'tags': self._extract_tags(f"{question} {answer}"), 'paraphrases': []}
        prompt = (
            f"Enrich this FAQ entry for a {vertical} chatbot.\n\n"
            f"Q: {question}\nA: {answer}\n\n"
            "Respond ONLY with JSON:\n"
            '{"tags": ["tag1", "tag2"], "paraphrases": ["alt question 1", "alt question 2"], '
            '"summary": "one sentence summary", "topic": "topic category"}'
        )
        try:
            resp   = _gemini_call(self.model, prompt, self._model_name)
            parsed = self._parse_json(resp.text or '')
            return parsed or {'tags': [], 'paraphrases': []}
        except Exception as e:
            log_crash(logger, 'AIEnrich', e)
            return {'tags': [], 'paraphrases': []}

    def _ai_enrich_batch(self, faq_pairs: List[tuple], vertical: str) -> List[Dict]:
        if not self.enabled or not self.model:
            return [
                {'tags': self._extract_tags(f"{q} {a}"), 'paraphrases': []}
                for q, a in faq_pairs
            ]
        entries = '\n\n'.join(
            f"FAQ {i + 1}:\nQ: {q}\nA: {a[:400]}"
            for i, (q, a) in enumerate(faq_pairs)
        )
        prompt = (
            f"Enrich these {len(faq_pairs)} FAQ entries for a {vertical} chatbot.\n\n"
            f"{entries}\n\n"
            "Respond ONLY with a JSON array — one object per FAQ, same order:\n"
            '[{"tags": ["t1","t2"], "paraphrases": ["alt q1","alt q2"], '
            '"summary": "one sentence", "topic": "category"}, ...]'
        )
        try:
            resp   = _gemini_call(self.model, prompt, self._model_name)
            parsed = self._parse_json(resp.text or '')
            if isinstance(parsed, list) and len(parsed) == len(faq_pairs):
                return parsed
            logger.warning(f"[AIEnrichBatch] unexpected response length, falling back")
        except Exception as e:
            log_crash(logger, 'AIEnrichBatch', e)
        return [{'tags': [], 'paraphrases': []} for _ in faq_pairs]

    def _quality_score(self, question: str, answer: str) -> float:
        q_len = len(question.split())
        a_len = len(answer.split())
        score = 0.0
        if 3  <= q_len <= 20:  score += 0.3
        if 10 <= a_len <= 200: score += 0.4
        if '?' in question:    score += 0.1
        if a_len > 20:         score += 0.2
        return round(min(score, 1.0), 2)

    def enrich_and_chunk(
        self,
        question: str,
        answer: str,
        vertical: str = 'general',
        client_id: Optional[str] = None,
    ) -> List[Dict]:
        enriched = self._ai_enrich(question, answer, vertical)
        chunks   = self._split_content(answer)
        results  = []
        for i, chunk in enumerate(chunks):
            paraphrase_str = ' '.join(enriched.get('paraphrases', [])[:3])
            embed_text = (
                f"{question} "
                f"{paraphrase_str} "
                f"{enriched.get('summary', '')} "
                f"{chunk}"
            ).strip()
            embedding  = _embed(embed_text, task='retrieval_document')
            results.append({
                'question':    question,
                'answer':      chunk,
                'chunk_index': i,
                'tags':        ' '.join(enriched.get('tags', [])),
                'paraphrases': enriched.get('paraphrases', []),
                'topic':       enriched.get('topic', ''),
                'quality':     self._quality_score(question, chunk),
                'embedding':   embedding,
                'vertical':    vertical,
                'client_id':   client_id,
            })
        return results

    def index_faqs(
        self,
        faqs: List[Dict],
        client_id: Optional[str] = None,
        vertical: str = 'general',
    ) -> int:
        import models as _m
        from cache_utils import bump_kb_version
        from itertools import islice

        def _batched(iterable: list, n: int):
            it = iter(iterable)
            while batch := list(islice(it, n)):
                yield batch

        valid_faqs = [
            f for f in faqs
            if str(f.get('question', '')).strip() and str(f.get('answer', '')).strip()
        ]

        indexed = 0
        for batch in _batched(valid_faqs, 5):
            pairs       = [(str(f['question']).strip(), str(f['answer']).strip()) for f in batch]
            enrichments = self._ai_enrich_batch(pairs, vertical)

            for faq, enriched in zip(batch, enrichments):
                try:
                    question = str(faq.get('question', '')).strip()
                    answer   = str(faq.get('answer', '')).strip()
                    kb_id    = faq.get('kb_id') or faq.get('id')
                    if not kb_id:
                        continue

                    chunks = self._split_content(answer)
                    for i, chunk in enumerate(chunks):
                        paraphrase_str = ' '.join(enriched.get('paraphrases', [])[:3])
                        embed_text = (
                            f"{question} {paraphrase_str} "
                            f"{enriched.get('summary', '')} {chunk}"
                        ).strip()
                        embedding = _embed(embed_text, task='retrieval_document')
                        chunk_data = {
                            'question':    question,
                            'answer':      chunk,
                            'chunk_index': i,
                            'tags':        ' '.join(enriched.get('tags', [])),
                            'paraphrases': enriched.get('paraphrases', []),
                            'topic':       enriched.get('topic', ''),
                            'quality':     self._quality_score(question, chunk),
                            'embedding':   embedding,
                            'vertical':    vertical,
                            'client_id':   client_id,
                        }
                        existing = _m.KbEntry.query.filter_by(
                            client_id=client_id, kb_id=kb_id,
                            chunk_index=i
                        ).first()
                        if existing:
                            for k, v in chunk_data.items():
                                setattr(existing, k, v)
                        else:
                            _m.db.session.add(_m.KbEntry(kb_id=kb_id, **chunk_data))

                    # FIX: this loop only upserts chunk_index 0..len(chunks)-1
                    # for the answer as it stands right now. If a previous
                    # index of this same FAQ produced MORE chunks (longer
                    # answer) than this one, the old trailing chunk rows
                    # were never touched — they just sat in the KB forever,
                    # still fully retrievable, still carrying whatever
                    # stale/incorrect content the answer used to have before
                    # it was edited. Delete anything left over beyond the
                    # current chunk count.
                    _m.KbEntry.query.filter(
                        _m.KbEntry.client_id == client_id,
                        _m.KbEntry.kb_id == kb_id,
                        _m.KbEntry.chunk_index >= len(chunks),
                    ).delete(synchronize_session=False)

                    indexed += 1
                except Exception as e:
                    log_crash(logger, 'IndexFAQ', e, faq_id=faq.get('kb_id'))

        if indexed:
            try:
                _m.db.session.commit()
                if client_id:
                    bump_kb_version(client_id)
                logger.info(f"[IndexFAQs] indexed={indexed} client={client_id}")
            except Exception as e:
                log_crash(logger, 'IndexFAQ/commit', e, client_id=client_id)
                _m.db.session.rollback()

        return indexed

    def approve_and_publish_gap(
        self,
        gap_id: int,
        answer: str,
        client_id: str,
        vertical: str = 'general',
    ) -> bool:
        try:
            import models as _m
            from cache_utils import bump_kb_version
            gap = _m.KbGap.query.filter_by(id=gap_id, client_id=client_id).first()
            if not gap:
                return False
            chunks = self.enrich_and_chunk(gap.question, answer, vertical, client_id)
            for chunk in chunks:
                entry = _m.KbEntry(kb_id=f"gap_{gap_id}", **chunk)
                _m.db.session.add(entry)
            gap.status     = 'resolved'
            gap.resolution = answer
            _m.db.session.commit()
            bump_kb_version(client_id)
            logger.info(f"[ApproveGap] gap_id={gap_id} client={client_id}")
            return True
        except Exception as e:
            log_crash(logger, 'ApproveGap', e, gap_id=gap_id, client_id=client_id)
            return False

    def draft_gap_answer(
        self,
        question: str,
        client_id: str,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
    ) -> Optional[str]:
        if not self.enabled or not self.model:
            return None
        prompt = (
            f"Draft a helpful, accurate answer to this customer question:\n\n"
            f"Q: {question}\n\n"
            "Write in a friendly, professional tone. 2–4 sentences. "
            "Acknowledge any uncertainty. Do not make up specific numbers or policies."
        )
        try:
            resp = _gemini_call(self.model, prompt, self._model_name)
            return (resp.text or '').strip() or None
        except Exception as e:
            log_crash(logger, 'DraftGap', e, client_id=client_id)
            return None

    def extract_lead_intent(
        self,
        message: str,
        conversation_snippet: str = '',
        vertical: str = 'general',
    ) -> Dict:
        # NOTE: priority defaults to 'med' on any failure path (no model,
        # empty context, parse failure) — never 'high'. A lead we couldn't
        # actually score should not silently outrank leads we genuinely
        # classified as high-priority.
        fallback = {'summary': '', 'priority': 'med'}
        if not self.enabled or not self.model:
            return fallback
        context = (conversation_snippet or message or '').strip()
        if not context:
            return fallback
        prompt = (
            f"A visitor just submitted a lead form on a {vertical} business's chatbot.\n\n"
            f"Their message / conversation context:\n\"{context[:1500]}\"\n\n"
            "In 2-3 sentences, summarize what this person wants and any "
            "urgency or buying signals you notice. Then classify priority:\n"
            "- high: ready to buy/book, urgent need, or explicit timeline\n"
            "- med: clearly interested but no urgency signal\n"
            "- low: vague inquiry, early research, or just browsing\n\n"
            "Respond ONLY with JSON:\n"
            '{"summary": "2-3 sentence summary", "priority": "high|med|low"}'
        )
        try:
            resp     = _gemini_call(self.model, prompt, self._model_name)
            parsed   = self._parse_json(resp.text or '')
            if not parsed or not parsed.get('summary'):
                return fallback
            priority = str(parsed.get('priority', 'med')).strip().lower()
            if priority not in ('high', 'med', 'low'):
                priority = 'med'
            return {'summary': str(parsed['summary'])[:500], 'priority': priority}
        except Exception as e:
            log_crash(logger, 'ExtractLeadIntent', e)
            return fallback

    def reindex_all_clients(self, app_context: Any = None) -> Dict[str, int]:
        try:
            import models as _m
            context = app_context or __import__('flask').current_app.app_context()
            results: Dict[str, int] = {}
            with context:
                clients = _m.db.session.query(_m.KbEntry.client_id).distinct().all()
                for (cid,) in clients:
                    entries = _m.KbEntry.query.filter_by(client_id=cid).all()
                    count = 0
                    for entry in entries:
                        # FIX: was f"{entry.question} {entry.answer}" — dropped
                        # the paraphrase expansion that index_faqs()/
                        # enrich_and_chunk() bake into the embed text at
                        # initial indexing time (Phase 6 accuracy work). Any
                        # full reindex (post-incident recovery, model
                        # migration) was silently regressing every client's
                        # retrieval quality back to bare question+answer
                        # matching. Reusing the already-stored
                        # entry.paraphrases costs no extra Gemini call. Note:
                        # the enrichment 'summary' isn't persisted as its own
                        # column today, so it still can't be recovered here —
                        # only a full re-index via index_faqs() restores that
                        # part too.
                        paraphrases = getattr(entry, 'paraphrases', None)
                        if isinstance(paraphrases, str):
                            try:
                                paraphrases = __import__('json').loads(paraphrases) if paraphrases else []
                            except Exception:
                                paraphrases = []
                        paraphrases = paraphrases or []
                        paraphrase_str = (
                            ' '.join(paraphrases[:3]) if isinstance(paraphrases, list) else ''
                        )
                        text = f"{entry.question} {paraphrase_str} {entry.answer}".strip()
                        vec  = _embed(text, task='retrieval_document')
                        if vec:
                            entry.embedding = vec
                            count += 1
                    _m.db.session.commit()
                    results[cid] = count
                    logger.info(f"[Reindex] client={cid} entries={count}")
            return results
        except Exception as e:
            log_crash(logger, 'Reindex', e)
            return {}

    def find_best_faq(
        self,
        query: str,
        faqs: List[Dict],
        threshold: float = 0.40,
    ) -> Optional[Dict]:
        candidates, scores = embedding_search(query, faqs, client_id=None)
        if candidates and scores and scores[0] >= threshold:
            return candidates[0]
        return None
