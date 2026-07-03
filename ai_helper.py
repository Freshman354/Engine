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
    PERSONALITIES,
    STAGE_SIGNALS,
    STAGE_ORDER,
    LEAD_NUDGE_MAX_PER_SESSION,
    LEAD_NUDGE_COOLDOWN_TURNS,
)
from pipeline.context import PipelineRequest, PipelineResult
from pipeline.stages.escalation  import check_escalation
from pipeline.stages.agent_actions import (
    client_has_active_actions,
    handle_pending_confirmation,
    try_dispatch_external_action,
)
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
    handle_detected_action,
)
from pipeline.stages.retrieval   import (
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
_EMAIL_RE   = re.compile(r'\b[\w.+-]+@[\w-]+\.[a-zA-Z]{2,}\b')
_PHONE_RE   = re.compile(r'\b(?:\+?\d[\d\s\-().]{7,14}\d)\b')
_NAME_RE    = re.compile(
    r"(?:i'?m|my name is|this is|call me)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
    re.IGNORECASE,
)
_URGENT_KW  = frozenset({'urgent', 'asap', 'immediately', 'today', 'right now', 'right away'})

# ── IDK response cache TTL ────────────────────────────────────────────────────
_REDIS_IDK_TTL_SEC = 900   # 15 minutes

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
                # Both providers get built if their keys are present,
                # regardless of which one is "active" right now — that's
                # what makes the admin dashboard's live switch actually
                # work without a restart. See AIHelper.__init__.
                _ai_helper_instance = AIHelper(
                    api_key    = api_key or os.environ.get('GEMINI_API_KEY', ''),
                    model_name = model_name or os.environ.get('GEMINI_MODEL', 'gemini-2.0-flash'),
                )
    return _ai_helper_instance


# ═════════════════════════════════════════════════════════════════════════════
# AIHelper
# ═════════════════════════════════════════════════════════════════════════════

class AIHelper:
    """
    Live AI-provider switch lives here. self.model, self.model_name,
    self._model_name, and self.provider are all @property — they check
    utils.get_ai_provider() FRESH on every access, not once at
    construction. This is deliberate: AIHelper is a process-lifetime
    singleton (see get_ai_helper() above), so if the choice of provider
    were baked into plain instance attributes set in __init__, toggling
    the switch in the admin dashboard would silently do nothing until
    the next deploy/restart. Building both clients up front (whichever
    are configured) and deciding which to use per-access is what makes
    the switch actually live.
    """

    def __init__(
        self,
        api_key:    str = '',
        model_name: str = 'gemini-2.0-flash',
    ) -> None:
        self.api_key           = api_key
        self._gemini_model_name = model_name
        self._genai_client      = None
        self._gemini_model_obj  = None   # client.models, or None if unavailable
        self._gemini_init_error = None

        openrouter_key = os.environ.get('OPENROUTER_API_KEY', '').strip()
        self._openrouter_configured = bool(openrouter_key)

        if api_key:
            try:
                self._genai_client = genai.Client(api_key=api_key)
                self._gemini_model_obj = self._genai_client.models
                logger.info(f"[AIHelper] google.genai ready model={model_name}")
            except Exception as e:
                log_crash(logger, 'AIHelper/init', e, model=model_name)
                self._gemini_init_error = e

        if self._openrouter_configured:
            logger.info(f"[AIHelper] OpenRouter configured, model={os.environ.get('OPENROUTER_MODEL', 'meta-llama/llama-4-maverick')}")

        # self.enabled = at least one provider is usable right now,
        # regardless of which one happens to be "active" — an admin
        # toggling the switch shouldn't need Gemini AND OpenRouter both
        # configured just for self.enabled to stay true.
        self.enabled = bool(self._gemini_model_obj) or self._openrouter_configured

        if not self.enabled:
            logger.warning(
                "[AIHelper] No usable AI provider configured (checked GEMINI_API_KEY "
                "and OPENROUTER_API_KEY) — running in static fallback mode. Retrieval "
                "(embedding search) still works; LLM generation disabled."
            )

    # ── Live provider switch ─────────────────────────────────────────────────

    @property
    def provider(self) -> str:
        """The CURRENTLY ACTIVE provider — re-checked on every access, not cached on self."""
        from utils import get_ai_provider
        chosen = get_ai_provider()
        # If the admin switched to a provider that isn't actually configured
        # here, fall back to whichever one IS, rather than silently going dark.
        if chosen == 'gemini' and not self._gemini_model_obj and self._openrouter_configured:
            return 'openrouter'
        if chosen == 'openrouter' and not self._openrouter_configured and self._gemini_model_obj:
            return 'gemini'
        return chosen

    @property
    def model(self) -> Optional[Any]:
        """
        The active provider's client object for this request. In
        openrouter mode this is a truthy sentinel (see class docstring on
        pipeline/stages/agent_actions.py and generation.py's `if model is
        None: return` gates) — utils.generate() ignores it entirely in
        that mode, calling OpenRouter directly instead.
        """
        if not self.enabled:
            return None
        if self.provider == 'openrouter':
            return AIHelper   # truthy, harmless — never dereferenced as a real client
        return self._gemini_model_obj

    @property
    def model_name(self) -> str:
        if self.provider == 'openrouter':
            from utils import OPENROUTER_MODEL
            return OPENROUTER_MODEL
        return self._gemini_model_name

    @property
    def _model_name(self) -> str:
        # Alias — some call sites use the underscored name.
        return self.model_name

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
        result['name'] = m.group(1).strip() if m else session_mem.get('name')
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
        if not email:
            logger.debug(f"[LeadPersist] no email in payload, skipping client={client_id}")
            return

        try:
            from models.leads import save_lead
            from models.clients import get_client_by_id

            lead_data = {
                'name':    payload.get('name') or '',
                'email':   email,
                'phone':   payload.get('phone') or '',
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
                f"email={email} stage={payload.get('purchase_stage')}"
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
        <td style="padding:8px 0;"><a href="mailto:{email}" style="color:#B8924A;">{email}</a></td></tr>
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;">Phone</td>
        <td style="padding:8px 0;">{phone}</td></tr>
    <tr><td style="padding:8px 0;color:#78716C;font-size:13px;vertical-align:top;">Message</td>
        <td style="padding:8px 0;font-style:italic;">&ldquo;{message[:300]}&rdquo;</td></tr>
  </table>
  <hr style="border:none;border-top:1px solid #E7E2DA;margin:20px 0;">
  <p style="font-size:13px;color:#78716C;">
    Reply directly to <a href="mailto:{email}" style="color:#B8924A;">{email}</a>
    to follow up.
  </p>
</body>
</html>"""

            text = (
                f"New lead from your chatbot\n\n"
                f"Name:    {name}\n"
                f"Email:   {email}\n"
                f"Phone:   {phone}\n"
                f"Message: {message[:300]}\n\n"
                f"Reply to {email} to follow up."
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
            msg['Reply-To'] = email          # reply goes to the lead, not Lumvi
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
        )

        # ── Response cache check ──────────────────────────────────────
        ctx.resp_cache_key = self._cache_key(
            user_message.lower().strip(),
            client_id or '',
            vertical,
            str(kb_version or ''),
        )
        cached = self._read_resp_cache(ctx.resp_cache_key)
        if cached:
            logger.debug(f"[Pipeline] cache hit trace={ctx.trace_id}")
            return cached

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

            # ── Pending external integration action (yes/no confirmation) ──
            # Checked BEFORE email_capture_pending / handoff_offered — a stray
            # "yes" while a Buy-Seat-style action confirmation is pending must
            # resolve the action, not get misrouted into the handoff flow.
            _pending_result = handle_pending_confirmation(
                client_id, session_id, ctx.session_mem, ctx.clean_message
            )
            if _pending_result is not None:
                if _pending_result.pop('clear_pending', False):
                    ctx.session_mem['pending_integration_action'] = None
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                return PipelineResult(
                    response=_pending_result['response'],
                    method=_pending_result['method'],
                    confidence=1.0,
                    action=_pending_result.get('action'),
                ).to_dict()

            # ── Email capture (waiting for email after handoff acceptance) ──
            # Checked BEFORE handoff_offered — a user replying "sure, sarah@email.com"
            # contains both an affirmative and an email. Without this guard the
            # handoff_offered arm would misroute it as a fresh handoff acceptance.
            if ctx.session_mem.get('email_capture_pending'):
                msg_lower   = ctx.clean_message.lower()
                email_match = _EMAIL_RE.search(ctx.clean_message)

                # User changed their mind
                if any(k in msg_lower for k in [
                    'no', 'nope', 'never mind', 'cancel', "don't", 'skip', 'forget it'
                ]):
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult.declined_handoff().to_dict()

                if email_match:
                    # Email captured — complete handoff and persist lead
                    ctx.session_mem['email']                 = email_match.group(0)
                    ctx.session_mem['email_capture_pending'] = False
                    ctx.session_mem['handoff_offered']       = False
                    handoff_payload = self._build_handoff_payload(
                        conversation_history, ctx.session_mem,
                        ctx.clean_message, session_id, 'accepted'
                    )
                    _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                    _BG_EXECUTOR.submit(
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

                # Couldn't parse email — re-ask once, stay in capture state
                return PipelineResult(
                    response="I didn't quite catch that — could you type out your email address?",
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

                    _have_email = bool(ctx.session_mem.get('email'))

                    if _have_email:
                        # Email already in session — confirm and close
                        handoff_payload = self._build_handoff_payload(
                            conversation_history, ctx.session_mem,
                            ctx.clean_message, session_id, 'accepted'
                        )
                        ctx.session_mem['handoff_offered'] = False
                        _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                        _BG_EXECUTOR.submit(
                            self._persist_lead_and_notify,
                            client_id, handoff_payload, vertical
                        )
                        return PipelineResult(
                            response=(
                                f"Perfect — someone from the team will be in touch "
                                f"at {ctx.session_mem['email']} shortly."
                            ),
                            method='handoff_accepted',
                            confidence=1.0,
                            handoff=handoff_payload,
                        ).to_dict()

                    # No email yet — pivot to capture before confirming
                    ctx.session_mem['email_capture_pending'] = True
                    _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult(
                        response=self._build_email_request(vertical),
                        method='email_capture_prompt',
                        confidence=1.0,
                    ).to_dict()

                if any(k in msg_lower for k in [
                    'no', 'nope', 'not now', 'no thanks', "don't", "i'm fine", 'im fine'
                ]):
                    ctx.session_mem['handoff_offered'] = False
                    _BG_EXECUTOR.submit(
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
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                if ctx.session_mem.get('email'):
                    _BG_EXECUTOR.submit(
                        self._persist_lead_and_notify,
                        client_id, handoff_payload, vertical
                    )
                return PipelineResult(
                    response=escalation_text,
                    method='escalation',
                    confidence=1.0,
                    handoff=handoff_payload,
                ).to_dict()

            # ── Load poor KB IDs ──────────────────────────────────────
            try:
                poor = get_poor_answers(client_id or '', limit=100)
                ctx.poor_kb_ids = {
                    str(p.get('kb_id', '')) for p in poor if p.get('kb_id')
                }
            except Exception:
                ctx.poor_kb_ids = set()

            # ── Intent detection ──────────────────────────────────────
            intent = detect_intent(
                ctx.clean_message, vertical, lead_triggers,
                model=None, skip_gemini=True,
            )

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
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                return PipelineResult(
                    response=action_data['response'],
                    method='action',
                    confidence=1.0,
                    is_lead=True,
                    lead_metadata=lead_info,
                    action=action_data,
                ).to_dict()

            # ── Tool dispatch exit ────────────────────────────────────
            if intent['intent'] == 'tool' and intent.get('tool'):
                tool_result = dispatch_tool(
                    intent['tool'], ctx.clean_message, client_id, ctx.session_mem
                )
                return PipelineResult(
                    response=tool_result.get('response', make_fallback(vertical)),
                    method='tool',
                    confidence=0.9 if tool_result.get('success') else 0.0,
                    action={'tool': intent['tool'], **tool_result},
                ).to_dict()

            # ── External integration dispatch (agency-configured client systems) ──
            # Cheap gate first — only clients with at least one configured
            # client_ext_integration_actions row pay for the Gemini tools call.
            if self.enabled and client_id and client_has_active_actions(client_id):
                _ext_result = try_dispatch_external_action(
                    self.model, self._model_name, client_id, session_id,
                    ctx.clean_message, conversation_history,
                )
                if _ext_result is not None:
                    if _ext_result.get('method') == 'external_action_confirm':
                        ctx.session_mem['pending_integration_action'] = _ext_result['pending']
                    _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)
                    return PipelineResult(
                        response=_ext_result['response'],
                        method=_ext_result['method'],
                        confidence=0.9,
                        action=_ext_result.get('action'),
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

            # ── Query resolution ──────────────────────────────────────
            ctx.search_query = resolve_query(ctx.clean_message, conversation_history)

            # ── Multi-intent decomposition ────────────────────────────
            sub_queries = decompose_intents(ctx.search_query)

            # ── Helper: run embedding search across sub-queries ───────
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
                    return merged_c, merged_s
                return embedding_search(query, faqs, client_id, ctx.poor_kb_ids)

            # ── Preflight embedding search ────────────────────────────
            ctx.candidates, ctx.vector_scores = _do_embedding_search(
                ctx.search_query, sub_queries
            )
            ctx.top_cosine = ctx.vector_scores[0] if ctx.vector_scores else 0.0

            # ── Query rewriting (Call 1) ──────────────────────────────
            if (self.enabled and self.model
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

            # ── Hybrid rerank (RRF) ───────────────────────────────────
            ctx.hybrid_ranked, ctx.hybrid_scores = hybrid_rerank(
                ctx.search_query, ctx.candidates, ctx.vector_scores, faqs
            )
            ctx.top_hybrid = ctx.hybrid_scores[0] if ctx.hybrid_scores else 0.0

            # ── Cross-encoder rerank ──────────────────────────────────
            if (self.enabled and self.model
                    and not ctx.call1_used
                    and ctx.top_cosine < 0.35
                    and len(ctx.hybrid_ranked) >= 2):
                ctx.hybrid_ranked = cross_encoder_rerank(
                    ctx.search_query, ctx.hybrid_ranked, self.model
                )
                ctx.call1_used = True

            # ── Context building + summarisation ──────────────────────
            _BG_EXECUTOR.submit(
                maybe_summarise, client_id, conversation_history, self.model
            )
            ctx.context_str = build_context(conversation_history, client_id, ctx.clean_message)

            # ── RAG qualification gate ─────────────────────────────────
            ctx.rag_qualified = ctx.top_cosine >= ctx.vector_threshold

            # ── Topic mismatch gate ───────────────────────────────────
            if ctx.rag_qualified and ctx.hybrid_ranked:
                top_faq = ctx.hybrid_ranked[0]
                overlap = topic_overlap = __import__(
                    'pipeline.stages.math_helpers', fromlist=['topic_overlap']
                ).topic_overlap
                if overlap(ctx.clean_message, top_faq.get('question', '')) < 0.05:
                    ctx.rag_qualified = False
                    logger.debug(
                        f"[Pipeline] topic mismatch gate fired trace={ctx.trace_id}"
                    )

            # ── FAQ-keyed semantic cache ───────────────────────────────
            _faq_cache_key: Optional[str] = None
            if ctx.rag_qualified and ctx.hybrid_ranked:
                _top_faq_id = str(
                    ctx.hybrid_ranked[0].get('kb_id',
                    ctx.hybrid_ranked[0].get('id', ''))
                )
                if _top_faq_id:
                    _faq_cache_key = self._cache_key(
                        _top_faq_id, client_id or '', vertical
                    )
                    _faq_cached = self._read_resp_cache(_faq_cache_key)
                    if _faq_cached:
                        logger.debug(
                            f"[Pipeline] FAQ-keyed cache hit trace={ctx.trace_id}"
                        )
                        return _faq_cached

            # ── Generation ────────────────────────────────────────────
            if ctx.rag_qualified and self.enabled and self.model:
                final, confidence, method = rag_generate_and_polish(
                    ctx.search_query,
                    ctx.context_str,
                    ctx.hybrid_ranked,
                    vertical,
                    ctx.session_mem,
                    ctx.is_sales_query,
                    self.model,
                    client_id=client_id,
                )
            elif self.enabled and self.model:
                final = dynamic_fallback(
                    ctx.search_query, vertical, ctx.session_mem,
                    self.model, self._model_name, client_id=client_id,
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
            _result_to_cache = PipelineResult(
                response=final,
                method=method,
                confidence=confidence,
                is_lead=is_lead,
                lead_metadata=lead_metadata,
            ).to_dict()

            if confidence >= ctx.confidence_high and ctx.resp_cache_key:
                _BG_EXECUTOR.submit(self._write_resp_cache, ctx.resp_cache_key, _result_to_cache)
                if _faq_cache_key:
                    _BG_EXECUTOR.submit(self._write_resp_cache, _faq_cache_key, _result_to_cache)
            elif method == 'dynamic_fallback_idk' and ctx.resp_cache_key:
                _BG_EXECUTOR.submit(
                    self._write_resp_cache_ttl,
                    ctx.resp_cache_key,
                    _result_to_cache,
                    _REDIS_IDK_TTL_SEC,
                )

            # ── KB gap recording ──────────────────────────────────────
            if method in IDK_METHODS_ALL:
                _BG_EXECUTOR.submit(
                    record_kb_gap, client_id or '', ctx.clean_message,
                    method, confidence, session_id
                )
                ctx.session_mem['handoff_offered'] = True

            # ── Lead nudge ────────────────────────────────────────────
            # Gated by count + cooldown (constants.py) — without this,
            # purchase_stage's one-way ratchet keeps is_lead True for the
            # rest of the session and this would fire on every single turn.
            trigger_lead_collection = False
            _have_email = bool(
                (lead_metadata or {}).get('email') or ctx.session_mem.get('email')
            )
            if is_lead and not _have_email:
                _nudge_count     = ctx.session_mem.get('lead_nudge_count', 0)
                _current_turn    = len(conversation_history)
                _last_nudge_turn = ctx.session_mem.get('lead_nudge_last_turn', -LEAD_NUDGE_COOLDOWN_TURNS)
                _cooldown_ok     = (_current_turn - _last_nudge_turn) >= LEAD_NUDGE_COOLDOWN_TURNS

                if _nudge_count < LEAD_NUDGE_MAX_PER_SESSION and _cooldown_ok:
                    nudge = self._build_lead_nudge(
                        lead_info=ctx.session_mem,
                        vertical=vertical,
                        purchase_stage=ctx.session_mem.get('purchase_stage'),
                        is_sales=ctx.is_sales_query,
                        lead_q3=lead_q3,
                    )
                    final += '\n\n' + nudge
                    trigger_lead_collection = True
                    ctx.session_mem['lead_nudge_count']     = _nudge_count + 1
                    ctx.session_mem['lead_nudge_last_turn'] = _current_turn

            # ── Session persistence ───────────────────────────────────
            if client_id and session_id:
                _BG_EXECUTOR.submit(persist_session, client_id, session_id, ctx.session_mem)

            # ── Handoff payload for IDK paths ─────────────────────────
            handoff = None
            if method in IDK_METHODS_ALL or ctx.session_mem.get('handoff_offered'):
                handoff = self._build_handoff_payload(
                    conversation_history, ctx.session_mem,
                    ctx.clean_message, session_id, method
                )
                # Persist lead if we already have their email (e.g. captured
                # in a prior turn and IDK fires later in the conversation).
                if ctx.session_mem.get('email'):
                    _BG_EXECUTOR.submit(
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
            resp = _gemini_call(self.model, prompt, self._model_name,
                                 client_id=client_id, endpoint='draft_gap_response')
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
        fallback = {'summary': '', 'priority': 'high'}
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
            priority = str(parsed.get('priority', 'high')).strip().lower()
            if priority not in ('high', 'med', 'low'):
                priority = 'high'
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
                        text = f"{entry.question} {entry.answer}"
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
