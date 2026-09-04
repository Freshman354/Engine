"""
shopify_billing.py
-------------------
Shopify App Pricing integration: Partner API client, plan-handle <->
plan_type mapping, and the hosted plan-selection page URL builder.

Used by:
  - blueprints/billing.py's upgrade_page() (rail detection) and its new
    shopify_pricing_return() route.
  - app.py's reconcile_shopify_subscriptions() (daily cron worker).

Deliberately separate from commerce_adapters.py: that module talks to
each shop's own Admin API using per-shop credentials (an OAuth access
token stored in client_integrations, or merchant-entered
shopify_client_id/secret for inventory features). This module instead
talks to Shopify's PARTNER API using ONE app-level credential
(SHOPIFY_PARTNER_API_TOKEN) that authenticates as the Lumvi Partner
organization itself, not as any individual merchant — that's simply
how Shopify App Pricing subscription state is queried; it isn't
available through the Admin API. See
https://shopify.dev/docs/apps/launch/billing/shopify-app-pricing and
https://shopify.dev/docs/api/partner/latest/active-subscription (both
re-checked as current at the time this module was written — Sept
2026). Confirmed on that pass:
  - Shopify App Pricing has sent NO subscription-change webhooks since
    April 28, 2026. activeSubscription (here) + the plan_handle/shop
    URL redirect params (handled in blueprints/billing.py) are the only
    two ways to learn subscription state now.
  - A single Shopify plan can be "monthly with a yearly option" — one
    plan_handle covers both billing cycles; activeSubscription's
    billingPeriod ('EVERY_30_DAYS' or 'ANNUAL') says which the merchant
    picked. So, unlike Flutterwave's FLW_PLAN_IDS_MONTHLY/_ANNUAL split,
    Lumvi only needs ONE Shopify plan handle per tier.
  - activeSubscription's legacySubscriptionId field is populated ONLY
    for subscriptions migrated from the old Billing API — for
    subscriptions created natively in Shopify App Pricing (which is all
    of Lumvi's, since Lumvi never used the Billing API), it is always
    null. So it cannot be used as a durable per-subscription identifier
    here. Instead, this module uses the shop's own GID
    ("gid://shopify/Shop/<id>") as the value stored in
    users.subscription_id for shopify_app_pricing rows — that's the
    value every future activeSubscription(appId, shopId) call needs
    anyway (there is at most one active Shopify App Pricing contract
    per shop per app, so the shop GID unambiguously identifies it).
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)

# ── Config — all set once in the Partner Dashboard / Dev Dashboard,  ───────
# never per-merchant:
#   SHOPIFY_APP_GID            This app's own GID, e.g. "gid://shopify/App/1234"
#                              (Partner Dashboard > app > API access).
#   SHOPIFY_PARTNER_ORG_ID     The numeric Partner organization id, from the
#                              partners.shopify.com/<org_id>/... URL.
#   SHOPIFY_PARTNER_API_TOKEN  A Partner API client access token with the
#                              "Manage apps" permission (Partner Dashboard >
#                              Settings > Partner API clients). Different
#                              credential than SHOPIFY_APP_CLIENT_ID/SECRET
#                              in app.py, which authenticate the merchant-
#                              facing OAuth app, not the Partner API.
#   SHOPIFY_APP_STORE_HANDLE   This app's Shopify App Store listing handle
#                              (the slug Shopify's hosted pricing page URL
#                              needs). NOT the same value as app.py's
#                              build_embed_activation_urls()'s theme-app-
#                              embed extension handle ("lumvi-chat-embed") —
#                              different handle, different Shopify concept.
SHOPIFY_APP_GID             = os.environ.get('SHOPIFY_APP_GID', '')
SHOPIFY_PARTNER_ORG_ID      = os.environ.get('SHOPIFY_PARTNER_ORG_ID', '')
SHOPIFY_PARTNER_API_TOKEN   = os.environ.get('SHOPIFY_PARTNER_API_TOKEN', '')
SHOPIFY_APP_STORE_HANDLE    = os.environ.get('SHOPIFY_APP_STORE_HANDLE', '')
SHOPIFY_PARTNER_API_VERSION = '2026-07'
# Matches commerce_adapters.ShopifyAdapter.API_VERSION / app.py's
# SHOPIFY_API_VERSION — kept as its own constant rather than importing
# either of those, to avoid this billing-only module depending on
# commerce_adapters.py (which pulls in merchant-inventory concerns this
# module has nothing to do with).
SHOPIFY_ADMIN_API_VERSION_FOR_SHOP_LOOKUP = '2026-07'

_PARTNER_GRAPHQL_URL = (
    f'https://partners.shopify.com/{SHOPIFY_PARTNER_ORG_ID}'
    f'/api/{SHOPIFY_PARTNER_API_VERSION}/graphql.json'
) if SHOPIFY_PARTNER_ORG_ID else ''

_ACTIVE_SUBSCRIPTION_QUERY = '''
query ActiveSubscription($appId: ID!, $shopId: ID!) {
  activeSubscription(appId: $appId, shopId: $shopId) {
    billingPeriod
    cancelAtEndOfCycle
    items {
      handle
    }
  }
}
'''

# ── Plan handle <-> plan_type mapping ───────────────────────────────────────
# SHOPIFY_APP_PRICING_PLAN_HANDLES env var format:
#   "ai_starter:<handle>,ai_growth:<handle>,ai_scale:<handle>"
# Mirrors blueprints/billing.py's _parse_plan_ids() shape/validation, one
# handle per tier (see module docstring for why no monthly/annual split
# is needed here the way FLW_PLAN_IDS_MONTHLY/_ANNUAL needs one).
_PLAN_TYPES = ('ai_starter', 'ai_growth', 'ai_scale')


def _parse_plan_handles() -> dict:
    raw = os.environ.get('SHOPIFY_APP_PRICING_PLAN_HANDLES', '')
    if not raw:
        logger.error('[ShopifyBilling] SHOPIFY_APP_PRICING_PLAN_HANDLES is not set — '
                      'no Shopify plan_handle will map to a plan_type')
        return {}
    result = {}
    for pair in raw.split(','):
        pair = pair.strip()
        if not pair:
            continue
        if ':' not in pair:
            logger.warning(f'[ShopifyBilling] malformed SHOPIFY_APP_PRICING_PLAN_HANDLES '
                            f'entry: {pair!r} (expected plan_type:handle)')
            continue
        plan_type, handle = pair.split(':', 1)
        plan_type = plan_type.strip().lower()
        if plan_type not in _PLAN_TYPES:
            logger.warning(f'[ShopifyBilling] unknown plan_type {plan_type!r} in '
                            f'SHOPIFY_APP_PRICING_PLAN_HANDLES')
            continue
        result[plan_type] = handle.strip()
    missing = [p for p in _PLAN_TYPES if p not in result]
    if missing:
        logger.error(f'[ShopifyBilling] SHOPIFY_APP_PRICING_PLAN_HANDLES missing '
                      f'entries for: {missing}')
    return result


def plan_handle_to_plan_type(plan_handle: str):
    """
    Reverse-maps a Shopify plan handle (from activeSubscription's item, or
    a plan_handle URL param) back to a Lumvi plan_type ('ai_starter' /
    'ai_growth' / 'ai_scale'), or None if it doesn't match any configured
    handle. Re-parses the env var on every call — same cost as billing.py's
    _parse_plan_ids(), called at most a few times per request or per cron
    tick, not worth caching.
    """
    if not plan_handle:
        return None
    for plan_type, handle in _parse_plan_handles().items():
        if handle == plan_handle:
            return plan_type
    return None


# ── Partner API ──────────────────────────────────────────────────────────

def _partner_graphql(query: str, variables: dict):
    """Returns the response's `data` dict, or None on any failure
    (missing config, network error, non-200, or a GraphQL `errors` array).
    Callers must treat None as "couldn't check" — NOT as "no subscription"
    — those are different things and must not be handled the same way."""
    if not (_PARTNER_GRAPHQL_URL and SHOPIFY_PARTNER_API_TOKEN and SHOPIFY_APP_GID):
        logger.error('[ShopifyBilling] Partner API not configured — need '
                      'SHOPIFY_PARTNER_ORG_ID, SHOPIFY_PARTNER_API_TOKEN, '
                      'and SHOPIFY_APP_GID all set')
        return None
    try:
        resp = requests.post(
            _PARTNER_GRAPHQL_URL,
            json={'query': query, 'variables': variables},
            headers={
                'X-Shopify-Access-Token': SHOPIFY_PARTNER_API_TOKEN,
                'Content-Type': 'application/json',
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(f'[ShopifyBilling] Partner API http_{resp.status_code}: '
                         f'{resp.text[:500]}')
            return None
        payload = resp.json()
        if payload.get('errors'):
            logger.error(f'[ShopifyBilling] Partner API errors: {payload["errors"]}')
            return None
        return payload.get('data')
    except requests.exceptions.RequestException as e:
        logger.error(f'[ShopifyBilling] Partner API request failed: {e}')
        return None
    except ValueError as e:  # resp.json() on a non-JSON body
        logger.error(f'[ShopifyBilling] Partner API returned unparseable response: {e}')
        return None


def get_active_subscription(shop_gid: str):
    """
    Returns (subscription, error) — a 2-tuple, matching this codebase's
    existing convention for a "found vs. confirmed-absent vs. couldn't
    check" result (see blueprints/cron.py::_check_cron_secret(), which
    returns (secret_str, error_response_or_None) for the same reason).

      (subscription_dict, None)   Confirmed active contract. subscription_dict
                                   is {'plan_handle', 'billing_period',
                                   'cancel_at_end_of_cycle'}.
      (None, None)                CONFIRMED no active contract for this shop —
                                   a real state (fresh install, cancelled,
                                   expired, or uninstalled).
      (None, <error string>)      The Partner API call itself failed or
                                   returned something unusable (bad config,
                                   network error, non-200, a GraphQL errors
                                   array, or an activeSubscription with no
                                   items). NOT a real state — callers must
                                   treat this as "couldn't check right now",
                                   never as "cancelled". See
                                   app.py::reconcile_shopify_subscriptions(),
                                   the one caller where conflating these two
                                   would mass-downgrade paying merchants on a
                                   transient Partner API outage.
    """
    data = _partner_graphql(
        _ACTIVE_SUBSCRIPTION_QUERY,
        {'appId': SHOPIFY_APP_GID, 'shopId': shop_gid},
    )
    if data is None:
        return None, 'partner_api_call_failed'
    sub = data.get('activeSubscription')
    if not sub:
        return None, None
    items = sub.get('items') or []
    if not items:
        logger.error(f'[ShopifyBilling] activeSubscription for shop={shop_gid} '
                      f'returned no items — treating as unusable, not as cancelled')
        return None, 'active_subscription_missing_items'
    return {
        'plan_handle':            items[0].get('handle'),
        'billing_period':         sub.get('billingPeriod'),
        'cancel_at_end_of_cycle': bool(sub.get('cancelAtEndOfCycle')),
    }, None


# ── Shop GID lookup (Admin API, per-shop token — one-time, at first  ───────
# subscription confirmation only; the reconciliation job never needs this,
# it reuses the shop GID already stored in users.subscription_id) ──────────

def fetch_shop_gid(shop_domain: str, access_token: str):
    """
    Resolves a shop's numeric ID via the Admin API and returns it as the
    GID activeSubscription's shopId argument needs
    ("gid://shopify/Shop/<id>"), or None on failure.

    Same shop.json call as app.py's _fetch_shopify_shop_email — reuses the
    per-shop access_token already stored in
    client_integrations.platform_config from the original OAuth token
    exchange (connect_shopify_callback), so this needs no credential of
    its own. Only called once, at the moment blueprints/billing.py's
    shopify_pricing_return() first confirms a merchant's subscription —
    the resulting GID is then persisted as users.subscription_id, so every
    later reconciliation check goes straight to the Partner API.
    """
    try:
        resp = requests.get(
            f'https://{shop_domain}/admin/api/'
            f'{SHOPIFY_ADMIN_API_VERSION_FOR_SHOP_LOOKUP}/shop.json',
            headers={'X-Shopify-Access-Token': access_token},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.error(f'[ShopifyBilling] shop.json fetch failed for {shop_domain}: '
                         f'http_{resp.status_code}')
            return None
        shop_id = ((resp.json() or {}).get('shop') or {}).get('id')
        if not shop_id:
            return None
        return f'gid://shopify/Shop/{shop_id}'
    except requests.exceptions.RequestException as e:
        logger.error(f'[ShopifyBilling] shop.json fetch failed for {shop_domain}: {e}')
        return None


# ── Hosted plan-selection page URL ────────────────────────────────────────

def pricing_plans_url(shop_domain: str):
    """
    Builds the URL that sends a merchant to Shopify's OWN hosted plan
    selection page — Lumvi renders no UI of its own for this, Shopify
    hosts it. Returns None if SHOPIFY_APP_STORE_HANDLE isn't configured
    or shop_domain is empty (caller must fail safe, not redirect to a
    broken URL — see blueprints/billing.py's upgrade_page()).

    ASSUMPTION FLAGGED FOR VERIFICATION WITH A LIVE STORE: the
    {store_handle} path segment is derived here by stripping
    ".myshopify.com" from shop_domain. Correct for the large majority of
    stores, but Shopify documents "store handle" as independently
    renameable from the *.myshopify.com subdomain for stores that have
    changed their handle since creation — not confirmed here against a
    store that has actually done that. Worst case on a mismatch is a 404
    on Shopify's side, not a wrong charge, but flagged rather than
    silently assumed correct.
    """
    if not (shop_domain and SHOPIFY_APP_STORE_HANDLE):
        return None
    store_handle = shop_domain.replace('.myshopify.com', '')
    return (
        f'https://admin.shopify.com/store/{store_handle}'
        f'/charges/{SHOPIFY_APP_STORE_HANDLE}/pricing_plans'
    )
