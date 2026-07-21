"""
shopify_connect.py
===================
Single connect step for Shopify that feeds client_integrations (webhooks.py)
so an agency only enters their Shopify credentials once:

  - client_integrations (webhooks.py) — powers tools.lookup_order /
    tools.search_products via commerce_adapters.py's live GraphQL reads
    (client credentials grant + auto-refresh — see
    commerce_adapters._get_shopify_access_token), plus inbound
    order-webhook signature verification.

Shopify deprecated static, paste-once Admin API access tokens for new
custom apps as of January 1, 2026 — a new custom app now issues a
client_id + client_secret instead, which commerce_adapters.py exchanges
for a short-lived (~24h) access token automatically, refreshing as needed.
This module just stores the client_id/client_secret pair; it never sees or
stores an access token itself.

enable_agent_actions is currently a NO-OP, left in the signature so
callers don't need updating twice. Agent Actions (pipeline/stages/
agent_actions.py) executes through GenericRESTAdapter
(pipeline/integration_adapter.py), which only supports static
header/query/basic auth — it has no OAuth token-refresh mechanism. Wiring
a client_id/client_secret pair into it would either silently break (an
expired token 24h later with no refresh) or misuse client_secret as a
static header value, which is worse than not offering it. Revisit once
GenericRESTAdapter supports refreshable auth.
"""
import logging

import webhooks as _wh

logger = logging.getLogger(__name__)


def connect_shopify(
    client_id: str,
    shop_domain: str,
    shopify_client_id: str,
    shopify_client_secret: str,
    webhook_secret: str,
    enable_order_lookup: bool = True,
    enable_inventory: bool = True,
    enable_agent_actions: bool = False,
) -> dict:
    """
    One connect step for Shopify — writes client_integrations (order-webhook
    verification, plus tools.lookup_order / tools.search_products's live
    reads via commerce_adapters.py).

    Args:
        client_id:               Lumvi client identifier
        shop_domain:              e.g. 'mystore.myshopify.com'
        shopify_client_id:        From a custom app created in Shopify's Dev
                                   Dashboard — as of Jan 1 2026, Shopify no
                                   longer issues a static token directly (see
                                   module docstring). Needs read_orders scope
                                   for order_lookup, read_products for
                                   inventory — grant both on the same app.
        shopify_client_secret:    Paired with shopify_client_id. Exchanged
                                   for an access token server-side by
                                   commerce_adapters.py as needed — never
                                   stored or used as a static header value.
        webhook_secret:           HMAC secret from Shopify's webhook setup.
                                   Still required — client_integrations.
                                   webhook_secret is NOT NULL regardless of
                                   whether webhook sync is this client's
                                   primary order-read path or just a fallback.
        enable_order_lookup:      Sets order_lookup_enabled in platform_config
        enable_inventory:         Sets inventory_enabled in platform_config
        enable_agent_actions:     Currently a no-op — see module docstring.
                                   Always returns agent_actions_integration_id:
                                   None with an explanatory entry in errors
                                   if True, rather than silently ignoring it.

    Returns:
        {success, client_integration: bool, agent_actions_integration_id: None,
         actions_created: 0, errors: [str]}
    """
    shop_domain            = (shop_domain or '').strip()
    shopify_client_id      = (shopify_client_id or '').strip()
    shopify_client_secret  = (shopify_client_secret or '').strip()
    errors = []

    if not shop_domain or not shopify_client_id or not shopify_client_secret or not webhook_secret:
        return {
            'success': False,
            'error': 'shop_domain, shopify_client_id, shopify_client_secret, and webhook_secret are all required.',
        }

    # client_integrations — read-merge-write, not a blind overwrite.
    # upsert_integration's ON CONFLICT does platform_config = EXCLUDED.
    # platform_config — a full replace, not a merge — so anything already
    # in platform_config that this call doesn't know about (set by a prior
    # webhook-only setup, or a prior connect_shopify call with different
    # flags) has to be preserved explicitly rather than clobbered.
    existing = _wh.get_integration(client_id, 'shopify')
    merged_config = dict((existing or {}).get('platform_config') or {})
    merged_config.update({
        'shop_domain':              shop_domain,
        'shopify_client_id':        shopify_client_id,
        'shopify_client_secret':    shopify_client_secret,
        'order_lookup_enabled':     enable_order_lookup,
        'inventory_enabled':        enable_inventory,
    })

    ok = _wh.upsert_integration(client_id, 'shopify', webhook_secret, merged_config)
    if not ok:
        errors.append('Could not save the Shopify order/inventory connection.')

    if enable_agent_actions:
        errors.append(
            "Agent Actions isn't available for Shopify yet — it needs a "
            "credential refresh mechanism this system doesn't have. Order "
            "lookup and product search were still connected."
        )

    return {
        'success':                       ok,
        'client_integration':            ok,
        'agent_actions_integration_id':  None,
        'actions_created':               0,
        'errors':                        errors,
    }
