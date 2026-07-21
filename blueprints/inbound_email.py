"""
blueprints/inbound_email.py
============================
Receives Brevo's inbound-parsing webhook and forwards cart-recovery
customer replies to the merchant's notification_email.

Routes
------
  POST /webhooks/inbound-email/<secret>    receive_inbound_email

One-time setup needed OUTSIDE this code (see the accompanying chat
message for the exact steps) — none of this is automatable from here:
  1. Add a subdomain distinct from the sending domain, e.g. reply.lumvi.net,
     and delegate its MX records to Brevo (Brevo requires the receiving
     domain to differ from whatever sends mail).
  2. In Brevo's dashboard (or POST /v3/webhooks), create an inbound webhook
     with domain=reply.lumvi.net and url=https://<app>/webhooks/inbound-email/<INBOUND_EMAIL_SECRET>.
  3. Set the INBOUND_EMAIL_SECRET env var to a long random value — it's
     the only auth Brevo's inbound webhook supports (no signature header),
     so it's embedded in the URL itself, matching Brevo's own documented
     pattern for this.

Payload shape (Brevo's documented inbound-parse schema — see
https://developers.brevo.com/docs/inbound-parse-webhooks): a JSON body
with a top-level "items" array; each item has Recipients (the actual
RCPT TO addresses, most reliable for extracting our per-cart local part),
From, ExtractedMarkdownMessage (the reply text with quoted history and
signature already stripped by Brevo's parser), RawTextBody as a fallback.
"""
import hmac
import os

from flask import Blueprint, jsonify, request, current_app
from flask_mail import Message as MailMessage

import models

inbound_email_bp = Blueprint('inbound_email', __name__)

# Injected by init_inbound_email() — same pattern as every other blueprint here.
_mail = None


def init_inbound_email(mail):
    global _mail
    _mail = mail


CART_RECOVERY_SENDER = 'notifications@lumvi.net'


def _check_inbound_secret(provided: str) -> bool:
    expected = os.environ.get('INBOUND_EMAIL_SECRET', '').strip()
    if not expected:
        current_app.logger.error(
            '[InboundEmail] INBOUND_EMAIL_SECRET env var not set — endpoint disabled for safety.'
        )
        return False
    return hmac.compare_digest(provided or '', expected)


@inbound_email_bp.route('/webhooks/inbound-email/<secret>', methods=['POST'])
def receive_inbound_email(secret):
    if not _check_inbound_secret(secret):
        current_app.logger.warning(
            f'[InboundEmail] rejected — bad secret from {request.remote_addr}'
        )
        return jsonify({'error': 'Unauthorized'}), 401

    body  = request.get_json(silent=True) or {}
    items = body.get('items') or []
    forwarded = skipped = errors = 0

    for item in items:
        try:
            recipients = item.get('Recipients') or []
            # Recipients is RCPT TO — the address actually used to reach us,
            # most reliable place to pull our per-cart local part from
            # (To: can show something else if the customer's client
            # rewrites display headers).
            local_part = None
            for addr in recipients:
                if '@' in addr:
                    local_part = addr.split('@', 1)[0].strip()
                    break
            if not local_part:
                skipped += 1
                continue

            cart = models.get_cart_by_reply_local_part(local_part)
            if not cart:
                current_app.logger.info(
                    f'[InboundEmail] no cart found for local_part={local_part}'
                )
                skipped += 1
                continue

            merchant_email = cart.get('notification_email')
            if not merchant_email:
                current_app.logger.warning(
                    f"[InboundEmail] cart={cart['id']} has no notification_email — can't forward"
                )
                skipped += 1
                continue

            business_name  = cart.get('business_name') or 'their store'
            customer_from  = item.get('From') or {}
            customer_email = customer_from.get('Address', '')
            customer_name  = customer_from.get('Name') or customer_email
            message_text   = (
                item.get('ExtractedMarkdownMessage')
                or item.get('RawTextBody')
                or '(no message body)'
            )

            html = f"""
            <div style="font-family:'DM Sans',Arial,sans-serif;max-width:540px;margin:0 auto;
                        background:#F7F4EF;padding:24px;border-radius:16px;">
              <p style="font-size:12px;color:#A8A29E;margin:0 0 16px;">
                This is a reply to an automated cart recovery email sent by
                Lumvi on behalf of {business_name}.
              </p>
              <div style="background:#fff;border:1px solid #E7E2DA;border-radius:12px;padding:16px;">
                <p style="font-size:13px;color:#57534E;margin:0 0 8px;">
                  <strong>{customer_name}</strong> &lt;{customer_email}&gt; replied:
                </p>
                <p style="font-size:14px;color:#1C1917;white-space:pre-wrap;margin:0;">{message_text}</p>
              </div>
              <p style="font-size:12px;color:#A8A29E;margin-top:16px;">
                Reply to this email to respond directly to the customer.
              </p>
            </div>
            """

            msg = MailMessage(
                subject    = f"Cart recovery reply from {customer_name}",
                sender     = f"Lumvi Cart Recovery <{CART_RECOVERY_SENDER}>",
                reply_to   = customer_email or None,
                recipients = [merchant_email],
                html       = html,
            )
            if _mail:
                _mail.send(msg)
            models.increment_reply_forwarded(cart['id'])
            forwarded += 1
            current_app.logger.info(
                f"[InboundEmail] forwarded cart={cart['id']} client={cart['client_id']} to={merchant_email}"
            )
        except Exception as e:
            current_app.logger.error(f'[InboundEmail] error processing item: {e}')
            errors += 1

    return jsonify({'success': True, 'forwarded': forwarded, 'skipped': skipped, 'errors': errors})
