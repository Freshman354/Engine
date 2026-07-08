"""
commerce_adapters.py
═══════════════════════════════════════════════════════════════════════════
Platform-agnostic interface for live commerce inventory lookups.

Layer 4 in ai_helper.py (availability resolution) calls
search_inventory_cached(client_id, entity) and never needs to know which
platform — Shopify, WooCommerce, or whatever's added later — is actually
behind the answer. Each platform gets one adapter class implementing the
same CommerceAdapter contract; the registry maps platform name → class.

Adding a new platform later means writing one adapter class and adding it
to ADAPTER_REGISTRY. Nothing in ai_helper.py or tools.py needs to change.

_get_inventory_integration() reads from the real client_integrations table
via webhooks.list_integrations(). Inventory capability is determined by
platform-specific signals in platform_config — no schema changes required.
═══════════════════════════════════════════════════════════════════════════
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from utils import get_logger

logger = get_logger('lumvi.commerce')


# ── Shared result shape — every adapter normalises into this ─────────────

@dataclass
class InventoryMatch:
    title:     str
    available: bool
    quantity:  Optional[int] = None
    variant:   Optional[str] = None   # e.g. "Red / Large" — None for simple products
    price:     Optional[str] = None


@dataclass
class InventoryResult:
    resolved: bool                                    # True = got a real platform answer
    matches:  List[InventoryMatch] = field(default_factory=list)
    error:    Optional[str] = None                     # set when resolved=False, for logging


@dataclass
class OrderInfo:
    id:                   str
    status:                str                         # normalised, lowercase
    total_amount:          Optional[str] = None
    currency:              Optional[str] = None
    updated_at:            Optional[str] = None
    financial_status:      Optional[str] = None         # Shopify-only; None for WooCommerce
    fulfillment_status:    Optional[str] = None          # Shopify-only; None for WooCommerce


@dataclass
class OrderLookupResult:
    resolved: bool                                     # True = got a real platform answer
    order:    Optional[OrderInfo] = None
    error:    Optional[str] = None                      # set when resolved=False, for logging/display


# ── Adapter contract ───────────────────────────────────────────────────

class CommerceAdapter(ABC):
    """
    One adapter per commerce platform. Layer 4 (in ai_helper.py via
    tools.check_commerce_inventory) only ever calls search_inventory().
    connect()/test_connection() are for the integrations.html connect flow
    and a future health-check badge — kept on the same contract so every
    platform exposes them consistently, even though the chat pipeline
    never touches them.
    """
    platform_name: str = "unknown"

    def __init__(self, credentials: Dict[str, str]):
        self.credentials = credentials or {}

    @abstractmethod
    def search_inventory(self, query: str) -> InventoryResult:
        """Search the platform's product catalog by name and return
        matching products with stock status. Must never raise — all
        failure modes (timeout, auth, rate limit) return
        InventoryResult(resolved=False, error=...) instead."""
        raise NotImplementedError

    @abstractmethod
    def get_order(self, order_id: str, customer_email: str = "") -> OrderLookupResult:
        """Look up a single order by the customer-facing order number
        (Shopify: order name like '#1001'; WooCommerce: order ID). If
        customer_email is given, only returns the order when it matches —
        this is the ownership check, not just a lookup. Must never raise —
        genuine platform failures (timeout, auth, rate limit) return
        OrderLookupResult(resolved=False, error=...); a real "no such
        order" or "doesn't belong to this email" is
        OrderLookupResult(resolved=True, order=None) instead, mirroring
        how search_inventory treats an empty match list as a resolved
        answer, not a failure."""
        raise NotImplementedError

    @abstractmethod
    def test_connection(self) -> bool:
        """Lightweight call to confirm stored credentials still work.
        Used to show a healthy/broken status badge on the integrations
        page, and to catch a revoked OAuth grant before it surfaces as
        a chat-time failure."""
        raise NotImplementedError


# ── Shopify adapter ────────────────────────────────────────────────────
# Auth: OAuth access token (NOT the existing webhook_secret — that secret
# only verifies inbound webhook payloads, it grants no API call rights).
# Credentials shape: {'shop_domain': 'mystore.myshopify.com', 'access_token': '...'}

class ShopifyAdapter(CommerceAdapter):
    platform_name = "shopify"
    API_VERSION   = "2024-10"

    def __init__(self, credentials: Dict[str, str]):
        super().__init__(credentials)
        self.shop_domain  = (credentials.get('shop_domain') or '').strip()
        self.access_token = (credentials.get('access_token') or '').strip()

    def search_inventory(self, query: str) -> InventoryResult:
        if not self.shop_domain or not self.access_token:
            return InventoryResult(resolved=False, error='shopify_credentials_missing')

        import requests
        url = f'https://{self.shop_domain}/admin/api/{self.API_VERSION}/graphql.json'
        gql = '''
        query SearchProducts($q: String!) {
          products(first: 5, query: $q) {
            edges {
              node {
                title
                variants(first: 10) {
                  edges {
                    node {
                      title
                      availableForSale
                      inventoryQuantity
                      price
                    }
                  }
                }
              }
            }
          }
        }
        '''
        try:
            resp = requests.post(
                url,
                json={'query': gql, 'variables': {'q': f'title:*{query}*'}},
                headers={'X-Shopify-Access-Token': self.access_token},
                timeout=5,
            )
            if resp.status_code == 429:
                logger.warning(f'[ShopifyAdapter] rate limited shop={self.shop_domain}')
                return InventoryResult(resolved=False, error='rate_limited')
            if resp.status_code == 401:
                return InventoryResult(resolved=False, error='unauthorized')
            if resp.status_code != 200:
                return InventoryResult(resolved=False, error=f'http_{resp.status_code}')

            data  = resp.json()
            edges = (((data.get('data') or {}).get('products') or {}).get('edges') or [])
            matches: List[InventoryMatch] = []
            for edge in edges:
                node  = edge.get('node', {})
                title = node.get('title', '')
                for v_edge in ((node.get('variants') or {}).get('edges') or []):
                    v = v_edge.get('node', {})
                    matches.append(InventoryMatch(
                        title=title,
                        available=bool(v.get('availableForSale')),
                        quantity=v.get('inventoryQuantity'),
                        variant=v.get('title') if v.get('title') not in (None, 'Default Title') else None,
                        price=v.get('price'),
                    ))
            return InventoryResult(resolved=True, matches=matches)

        except requests.exceptions.Timeout:
            return InventoryResult(resolved=False, error='timeout')
        except Exception as e:
            logger.error(f'[ShopifyAdapter] search_inventory error: {e}')
            return InventoryResult(resolved=False, error=str(e))

    def get_order(self, order_id: str, customer_email: str = "") -> OrderLookupResult:
        if not self.shop_domain or not self.access_token:
            return OrderLookupResult(resolved=False, error='shopify_credentials_missing')

        import requests
        url = f'https://{self.shop_domain}/admin/api/{self.API_VERSION}/graphql.json'
        # Customers reference the order NAME shown at checkout (e.g. "#1001"
        # or "1001"), not Shopify's internal numeric/gid ID.
        name = order_id if str(order_id).startswith('#') else f'#{order_id}'
        gql = '''
        query LookupOrder($q: String!) {
          orders(first: 1, query: $q) {
            edges {
              node {
                name
                displayFinancialStatus
                displayFulfillmentStatus
                cancelledAt
                updatedAt
                totalPriceSet { presentmentMoney { amount currencyCode } }
                email
              }
            }
          }
        }
        '''
        try:
            resp = requests.post(
                url,
                json={'query': gql, 'variables': {'q': f'name:{name}'}},
                headers={'X-Shopify-Access-Token': self.access_token},
                timeout=5,
            )
            if resp.status_code == 429:
                logger.warning(f'[ShopifyAdapter] rate limited shop={self.shop_domain}')
                return OrderLookupResult(resolved=False, error='rate_limited')
            if resp.status_code == 401:
                return OrderLookupResult(resolved=False, error='unauthorized')
            if resp.status_code != 200:
                return OrderLookupResult(resolved=False, error=f'http_{resp.status_code}')

            data  = resp.json()
            edges = (((data.get('data') or {}).get('orders') or {}).get('edges') or [])
            if not edges:
                return OrderLookupResult(resolved=True, order=None)  # genuinely not found

            node = edges[0].get('node', {})
            if customer_email and (node.get('email') or '').lower() != customer_email.lower():
                return OrderLookupResult(resolved=True, order=None)  # exists, but not theirs

            status = 'cancelled' if node.get('cancelledAt') else (
                (node.get('displayFulfillmentStatus') or 'unfulfilled').lower()
            )
            money = (node.get('totalPriceSet') or {}).get('presentmentMoney') or {}
            return OrderLookupResult(resolved=True, order=OrderInfo(
                id=node.get('name', order_id),
                status=status,
                total_amount=money.get('amount'),
                currency=money.get('currencyCode'),
                updated_at=node.get('updatedAt'),
                financial_status=(node.get('displayFinancialStatus') or '').lower() or None,
                fulfillment_status=(node.get('displayFulfillmentStatus') or '').lower() or None,
            ))

        except requests.exceptions.Timeout:
            return OrderLookupResult(resolved=False, error='timeout')
        except Exception as e:
            logger.error(f'[ShopifyAdapter] get_order error: {e}')
            return OrderLookupResult(resolved=False, error=str(e))

    def test_connection(self) -> bool:
        if not self.shop_domain or not self.access_token:
            return False
        import requests
        try:
            resp = requests.get(
                f'https://{self.shop_domain}/admin/api/{self.API_VERSION}/shop.json',
                headers={'X-Shopify-Access-Token': self.access_token},
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False


# ── WooCommerce adapter ────────────────────────────────────────────────
# Auth: REST API Consumer Key/Secret, generated in WP Admin → WooCommerce →
# Settings → Advanced → REST API. Pasted in directly — same UX pattern as
# the existing webhook_secret flow, no OAuth redirect needed.
# Credentials shape: {'store_url': 'https://mystore.com', 'consumer_key': '...',
#                      'consumer_secret': '...'}

class WooCommerceAdapter(CommerceAdapter):
    platform_name = "woocommerce"

    def __init__(self, credentials: Dict[str, str]):
        super().__init__(credentials)
        self.store_url        = (credentials.get('store_url') or '').strip().rstrip('/')
        self.consumer_key     = (credentials.get('consumer_key') or '').strip()
        self.consumer_secret  = (credentials.get('consumer_secret') or '').strip()

    def search_inventory(self, query: str) -> InventoryResult:
        if not self.store_url or not self.consumer_key or not self.consumer_secret:
            return InventoryResult(resolved=False, error='woocommerce_credentials_missing')

        import requests
        url = f'{self.store_url}/wp-json/wc/v3/products'
        try:
            resp = requests.get(
                url,
                params={'search': query, 'per_page': 5},
                auth=(self.consumer_key, self.consumer_secret),
                timeout=5,
            )
            if resp.status_code == 401:
                return InventoryResult(resolved=False, error='unauthorized')
            if resp.status_code != 200:
                return InventoryResult(resolved=False, error=f'http_{resp.status_code}')

            products = resp.json()
            if not isinstance(products, list):
                return InventoryResult(resolved=False, error='unexpected_response_shape')

            matches: List[InventoryMatch] = []
            for p in products:
                stock_status = p.get('stock_status')  # 'instock' | 'outofstock' | 'onbackorder'
                matches.append(InventoryMatch(
                    title=p.get('name', ''),
                    available=(stock_status == 'instock'),
                    quantity=p.get('stock_quantity'),
                    price=p.get('price') or None,
                ))
            return InventoryResult(resolved=True, matches=matches)

        except requests.exceptions.Timeout:
            return InventoryResult(resolved=False, error='timeout')
        except Exception as e:
            logger.error(f'[WooCommerceAdapter] search_inventory error: {e}')
            return InventoryResult(resolved=False, error=str(e))

    def get_order(self, order_id: str, customer_email: str = "") -> OrderLookupResult:
        if not self.store_url or not self.consumer_key or not self.consumer_secret:
            return OrderLookupResult(resolved=False, error='woocommerce_credentials_missing')

        import requests
        # WooCommerce's REST 'id' is the internal post ID, which is what
        # customers see as their order number by default. Stores using a
        # custom order-numbering plugin may show a different number — if
        # that becomes an issue for a given client, add a
        # GET /orders?search=<number> fallback here.
        url = f'{self.store_url}/wp-json/wc/v3/orders/{order_id}'
        try:
            resp = requests.get(
                url,
                auth=(self.consumer_key, self.consumer_secret),
                timeout=5,
            )
            if resp.status_code == 404:
                return OrderLookupResult(resolved=True, order=None)  # genuinely not found
            if resp.status_code == 401:
                return OrderLookupResult(resolved=False, error='unauthorized')
            if resp.status_code != 200:
                return OrderLookupResult(resolved=False, error=f'http_{resp.status_code}')

            data = resp.json()
            order_email = ((data.get('billing') or {}).get('email') or '')
            if customer_email and order_email.lower() != customer_email.lower():
                return OrderLookupResult(resolved=True, order=None)  # exists, but not theirs

            return OrderLookupResult(resolved=True, order=OrderInfo(
                id=str(data.get('id', order_id)),
                status=data.get('status', 'unknown'),
                total_amount=data.get('total'),
                currency=data.get('currency'),
                updated_at=data.get('date_modified'),
            ))

        except requests.exceptions.Timeout:
            return OrderLookupResult(resolved=False, error='timeout')
        except Exception as e:
            logger.error(f'[WooCommerceAdapter] get_order error: {e}')
            return OrderLookupResult(resolved=False, error=str(e))

    def test_connection(self) -> bool:
        if not self.store_url or not self.consumer_key or not self.consumer_secret:
            return False
        import requests
        try:
            resp = requests.get(
                f'{self.store_url}/wp-json/wc/v3/products',
                params={'per_page': 1},
                auth=(self.consumer_key, self.consumer_secret),
                timeout=5,
            )
            return resp.status_code == 200
        except Exception:
            return False


# ── Registry ────────────────────────────────────────────────────────────
# Adding a platform later: write the adapter class above, add one line here.
# Nothing in ai_helper.py, tools.py, or the Layer 4 pipeline needs to change.

ADAPTER_REGISTRY: Dict[str, type] = {
    'shopify':     ShopifyAdapter,
    'woocommerce': WooCommerceAdapter,
}


def get_adapter_for_client(client_id: str) -> Optional[CommerceAdapter]:
    """
    Returns a ready-to-use adapter for whichever commerce platform this
    client has connected with inventory capability — or None if they
    haven't connected one.

    Shopify:     requires 'inventory_enabled': True and 'access_token' in
                 platform_config. The basic Shopify connection (order webhooks
                 only) does NOT satisfy this — the agency must go through the
                 OAuth flow (/api/integrations/shopify/oauth/start) to grant
                 read_products + read_inventory scope.
    WooCommerce: requires 'consumer_key' and 'consumer_secret' in
                 platform_config. Any active WooCommerce row has these.
    """
    integration = _get_inventory_integration(client_id)
    if not integration:
        return None

    platform    = integration.get('platform')
    adapter_cls = ADAPTER_REGISTRY.get(platform)
    if not adapter_cls:
        logger.warning(f'[CommerceAdapters] no adapter registered for platform={platform}')
        return None

    return adapter_cls(integration.get('credentials', {}))


def _get_inventory_integration(client_id: str) -> Optional[Dict]:
    """
    Returns the first active integration for this client that has inventory
    capability, or None if none exists.

    Inventory capability is determined by platform-specific signals in
    platform_config — not a dedicated column (none exists in the schema):

      Shopify:     platform_config must contain 'inventory_enabled': True
                   and a non-empty 'access_token'. The basic Shopify row
                   (order webhooks only) has neither — a client can have
                   both without conflict since the same row gets extended
                   in-place when the OAuth flow completes.

      WooCommerce: platform_config must contain 'consumer_key' and
                   'consumer_secret'. These are required by the connect form,
                   so any active WooCommerce row is inventory-capable by
                   default.
    """
    import webhooks as _wh
    for row in _wh.list_integrations(client_id):
        platform = row.get('platform', '')
        cfg      = row.get('platform_config', {}) or {}

        if platform == 'shopify':
            if cfg.get('inventory_enabled') and cfg.get('access_token'):
                return {
                    'platform':    'shopify',
                    'credentials': {
                        'shop_domain':  cfg.get('shop_domain', ''),
                        'access_token': cfg.get('access_token', ''),
                    },
                }

        elif platform == 'woocommerce':
            if cfg.get('consumer_key') and cfg.get('consumer_secret'):
                return {
                    'platform':    'woocommerce',
                    'credentials': {
                        'store_url':       cfg.get('store_url', ''),
                        'consumer_key':    cfg.get('consumer_key', ''),
                        'consumer_secret': cfg.get('consumer_secret', ''),
                    },
                }

    return None


def _get_order_integration(client_id: str) -> Optional[Dict]:
    """
    Returns the first active integration for this client that has
    order-lookup capability, or None if none exists.

    Shopify:     platform_config must contain 'order_lookup_enabled': True
                 and a non-empty 'access_token'. Gated on its own explicit
                 flag rather than piggybacking on 'inventory_enabled' — an
                 agency may want the bot answering "is my order shipped"
                 without also exposing product/stock search, or vice versa.
                 (This assumes the OAuth app's granted scopes include
                 read_orders whenever access_token is present — worth
                 confirming against whatever scope bundle
                 /api/integrations/shopify/oauth/start actually requests;
                 if orders and inventory turn out to be separate OAuth
                 grants, this flag alone won't be enough.)

    WooCommerce: same as inventory — platform_config must contain
                 'consumer_key' and 'consumer_secret'. WooCommerce's REST
                 API key auth doesn't have Shopify-style scoped grants, so
                 any active row can already read orders; no extra flag.
    """
    import webhooks as _wh
    for row in _wh.list_integrations(client_id):
        platform = row.get('platform', '')
        cfg      = row.get('platform_config', {}) or {}

        if platform == 'shopify':
            if cfg.get('order_lookup_enabled') and cfg.get('access_token'):
                return {
                    'platform':    'shopify',
                    'credentials': {
                        'shop_domain':  cfg.get('shop_domain', ''),
                        'access_token': cfg.get('access_token', ''),
                    },
                }

        elif platform == 'woocommerce':
            if cfg.get('consumer_key') and cfg.get('consumer_secret'):
                return {
                    'platform':    'woocommerce',
                    'credentials': {
                        'store_url':       cfg.get('store_url', ''),
                        'consumer_key':    cfg.get('consumer_key', ''),
                        'consumer_secret': cfg.get('consumer_secret', ''),
                    },
                }

    return None


def get_order_adapter_for_client(client_id: str) -> Optional[CommerceAdapter]:
    """
    Returns a ready-to-use adapter for whichever commerce platform this
    client has connected with order-lookup capability — or None if they
    haven't. Mirrors get_adapter_for_client(), gated on
    _get_order_integration() instead of _get_inventory_integration().
    """
    integration = _get_order_integration(client_id)
    if not integration:
        return None

    platform    = integration.get('platform')
    adapter_cls = ADAPTER_REGISTRY.get(platform)
    if not adapter_cls:
        logger.warning(f'[CommerceAdapters] no adapter registered for platform={platform}')
        return None

    return adapter_cls(integration.get('credentials', {}))


def get_order_management_url(client_id: str) -> Optional[str]:
    """
    Returns the client's configured self-service order page (e.g. their
    Shopify/WooCommerce customer account "my orders" page, or a support/
    returns portal) if one is set — for redirecting cancellation and
    refund requests, the same way appointment booking redirects to the
    client's real Calendly/Acuity/Square page instead of Lumvi attempting
    the action itself. See tools.py's cancel_order.

    Unlike order lookup, this needs no live API credentials — it's just a
    link an agency pastes in — so it isn't gated through
    _get_order_integration(); any active integration (of any platform)
    with 'order_management_url' set in platform_config qualifies.
    """
    import webhooks as _wh
    for row in _wh.list_integrations(client_id):
        cfg = row.get('platform_config', {}) or {}
        url = cfg.get('order_management_url')
        if url:
            return url
    return None


# ── In-process inventory cache ─────────────────────────────────────────
# 60s TTL per client_id+query. Both Shopify (cost-based GraphQL throttling)
# and self-hosted WooCommerce stores can be hit hard if a popular product
# gets asked about repeatedly — this absorbs that without adding Redis
# as a hard dependency for what's a short-lived, low-stakes cache.
# NOTE: per-process only. Fine for a single Railway instance; if Lumvi
# ever runs multiple workers/dynos, move this to Redis (already used
# elsewhere in this codebase) so cache hits are shared across processes.

_INVENTORY_CACHE: Dict[str, tuple] = {}   # cache_key -> (InventoryResult, expiry_ts)
_CACHE_TTL_SEC = 60


def search_inventory_cached(client_id: str, query: str) -> InventoryResult:
    """
    Entry point for Layer 4. Checks the cache first, then the client's
    connected adapter (if any). Returns resolved=False with no error
    when no adapter is connected — that's the expected common case, not
    a failure.
    """
    cache_key = f'{client_id}:{query.lower().strip()}'
    cached = _INVENTORY_CACHE.get(cache_key)
    if cached and cached[1] > time.time():
        return cached[0]

    adapter = get_adapter_for_client(client_id)
    if not adapter:
        return InventoryResult(resolved=False, error='no_adapter_connected')

    result = adapter.search_inventory(query)
    if result.resolved:
        _INVENTORY_CACHE[cache_key] = (result, time.time() + _CACHE_TTL_SEC)
    return result


# ── In-process order-lookup cache ──────────────────────────────────────
# Same 60s-TTL, per-process pattern as the inventory cache above, keyed by
# client_id+order_id+customer_email instead of client_id+query. Kept as a
# separate dict rather than sharing _INVENTORY_CACHE so the two caches can
# be tuned/cleared independently — order status changes (shipped,
# delivered) matter more than a 60s-stale stock count for most support
# conversations, so this is a natural place to shorten the TTL later
# without touching inventory search.

_ORDER_CACHE: Dict[str, tuple] = {}   # cache_key -> (OrderLookupResult, expiry_ts)


def lookup_order_live(client_id: str, order_id: str, customer_email: str = "") -> OrderLookupResult:
    """
    Entry point for tools.lookup_order(). Checks the cache first, then the
    client's connected adapter (if any). Returns resolved=False with no
    error when no adapter is connected — the expected case for clients who
    only have inbound webhook sync configured, not live order-read access;
    callers should fall back to Lumvi's own synced `orders` table in that
    case (see tools.py).
    """
    cache_key = f'{client_id}:{order_id.lower().strip()}:{customer_email.lower().strip()}'
    cached = _ORDER_CACHE.get(cache_key)
    if cached and cached[1] > time.time():
        return cached[0]

    adapter = get_order_adapter_for_client(client_id)
    if not adapter:
        return OrderLookupResult(resolved=False, error='no_adapter_connected')

    result = adapter.get_order(order_id, customer_email)
    if result.resolved:
        _ORDER_CACHE[cache_key] = (result, time.time() + _CACHE_TTL_SEC)
    return result
