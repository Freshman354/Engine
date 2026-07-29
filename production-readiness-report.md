# Production Readiness Report — Lumvi Shopify Native App

**Scope:** everything built across Phases 0–5 of this migration (`app.py`, `webhooks.py`,
`commerce_adapters.py`, `models/__init__.py`, `models/users.py`, `blueprints/auth.py`,
templates, the Shopify CLI extension). Read fresh for this report, not recalled from
earlier summaries in this conversation — several findings below were not previously
flagged.

**Posture of this report:** written as a reviewer, not as the author defending the work.
Where something is genuinely solid, it's marked as a pass and left alone — not every
finding is a problem, and padding this with manufactured concerns would make the real
ones harder to see.

---

## Summary

| Severity | Count |
|---|---|
| 🔴 Blocker | 4 |
| 🟠 Warning | 6 |
| 🟡 Recommendation | 5 |
| ✅ Verified sound | 9 |

The single most important finding is **B1** — a cross-tenant webhook replay gap that's a
direct, previously-unnoticed consequence of the Phase 0 `webhook_secret` fix. Fix that
before anything else here.

---

## 🔴 Blockers

### B1 — Cross-tenant webhook replay (multi-tenancy + webhook verification)

**Where:** `webhooks.py`, `handle_shopify_webhook()`, the signature check at line ~937.

Phase 0 fixed `webhook_secret` to store `SHOPIFY_APP_CLIENT_SECRET` — correct, since
that's what Shopify actually signs app-registered webhooks with. But that means **every
Shopify-connected client now shares the exact same `webhook_secret`**. The handler verifies
the HMAC against that shared secret and nothing else — it never checks that the payload's
originating shop actually matches the `client_id` in the URL path
(`/webhooks/shopify/<client_id>`).

Concretely: any validly-signed webhook Shopify ever sent to *any* merchant on this app —
captured from a log, a proxy, a misconfigured client, anything — has a signature that
verifies successfully if replayed against a **different** client's webhook URL, because
the check only proves "Shopify signed this with the app secret," not "Shopify signed this
*for this shop*." `_upsert_order` would then happily write client A's order data into
client B's `orders` table (`_normalise_shopify_order` never cross-checks the shop either).

**Fix:** Shopify sends `X-Shopify-Shop-Domain` on every webhook request. Read it, compare
against the `shop_domain` stored in that client's `platform_config`, reject on mismatch —
before, not instead of, the HMAC check. Small fix, does not touch the encryption/secret
design from Phase 0.

**Why this wasn't caught earlier:** Phase 0's fix was reviewed for correctness in
isolation (does the stored secret match what Shopify actually signs with — yes) but not
for this second-order consequence (a shared secret removes the per-client discrimination
the old random-per-row secret used to provide as a side effect, even though that was never
its intended purpose either).

---

### B2 — REST Admin API calls, where GraphQL is now mandatory for new public apps

**Where:** `app.py:2062` (`_fetch_shopify_shop_email`), `commerce_adapters.py:398`
(`ShopifyAdapter.test_connection()`).

Both hit `GET /admin/api/{version}/shop.json` — REST. As of April 2026, Shopify requires
all **new public apps** to be built exclusively on the GraphQL Admin API; REST is
legacy-only. Every other Shopify API call in this codebase (`search_inventory`,
`get_order`, `register_webhooks`) already uses GraphQL — these two REST calls are both
things added in this migration, not inherited from the original code.

**Fix:** both are simple one-field lookups (`shop { email }`, a connectivity check) —
trivial to rewrite as GraphQL queries against `/admin/api/{version}/graphql.json`.

**Scope caveat:** this only blocks *public App Store listing*. A direct-install-link /
custom-distributed app (the model this migration was built around — see the Phase 3
non-embedded decision) isn't going through that review, so this doesn't block launch, only
a future public listing.

---

### B3 — Compliance webhooks don't perform the action they claim to

**Where:** `webhooks.py`, `handle_shopify_compliance_webhook()`.

This was flagged as a known limitation back in Phase 2, but it needs to be named as a
blocker here, not a footnote: per Shopify's current review documentation, **"the most
common first-time submission failure is a webhook endpoint that returns 200 OK without
actually doing anything."** That's exactly what this endpoint does — verifies, logs,
returns 200. `customers/redact` doesn't redact. `shop/redact` doesn't erase. Shopify's
review process specifically tests these, not just checks that the URL is registered.

**Not a regression, not new information** — but the earlier framing ("a deliberate
boundary, not an oversight") undersold how directly this blocks public listing
specifically. For a direct-install app not going through App Store review, this is a real
compliance/legal exposure rather than a review blocker — either way it needs an actual
decision, not indefinite deferral.

---

### B4 — Manual shop-domain entry conflicts with App Store installation rules

**Where:** `app.py`, `connect_shopify_install()` — the `shop_domain` form field path (used
by `onboarding.html`/`dashboard_enterprise.html`/`integrations.html`'s "Connect Shopify"
buttons when a merchant types their store domain in).

Current Shopify App Store rules: **"Installation must be initiated only from
Shopify-owned surfaces, apps can't ask merchants to manually enter a myshopify.com URL."**
The self-serve flow built in Phase 1 does exactly that — a text field where the merchant
types their shop domain, which then kicks off the OAuth redirect.

This is fine, even necessary, for the direct-install-link model this was built for (a
merchant with an existing Lumvi account connecting a store has to identify which store
somehow). It becomes a blocker only if this ever goes up for public App Store listing —
at that point the install must originate from Shopify's own "Install app" button or a
Shopify-generated install link, not a form field in your own UI.

---

## 🟠 Warnings

### W1 — `register_webhooks()` has no backoff on rate limiting

**Where:** `commerce_adapters.py`, `ShopifyAdapter.register_webhooks()`.

Loops through 8 topics with a sequential `requests.post` each, zero delay between calls,
zero retry on a 429. `search_inventory`/`get_order` at least *detect* a 429 (log + fail
gracefully); this method does the same detection but with no backoff-and-retry — and
because the 8 calls fire back-to-back with no spacing, a rate-limit hit on call 1 will
very likely cascade through calls 2–8 too, silently leaving most/all topics unregistered
for that merchant while `app.py` still tells them they're connected (Phase 2's design
deliberately doesn't fail the whole OAuth flow over a registration hiccup — reasonable,
but it means this failure mode is currently invisible to the merchant and easy to miss in
logs unless someone's specifically watching for `failed_topics=[...]` warnings).

**Fix:** at minimum, a short delay between the 8 calls; ideally exponential backoff on 429
specifically, distinct from other failure types.

### W2 — No caching on the highest-traffic new endpoint

**Where:** `app.py`, `/api/shopify/resolve-client`.

Called once per storefront page load, for every visitor, on every connected Shopify
store — this is now potentially the highest-QPS endpoint in the whole migration, and it's
a raw DB query (`get_client_id_by_shopify_shop`) with no caching layer, unlike
`_get_inventory_integration`/`_get_order_integration` in `commerce_adapters.py`, which do
use a 60-second in-process cache for comparable lookups. `cache_utils.py` is already
imported in `app.py` for other purposes — this endpoint doesn't use it.

**Fix:** short-TTL cache (shop_domain → client_id changes rarely — connect/disconnect
events only) using the existing `cache_utils` pattern already established elsewhere in
this codebase.

### W3 — Sequential webhook registration blocks the OAuth redirect

**Where:** `app.py`, `connect_shopify_callback()`, the `register_webhooks()` call.

8 synchronous HTTP round-trips to Shopify happen inline, in the same request that's
supposed to redirect the merchant back to Lumvi. At ~200–500ms per call this adds
1.5–4 seconds to the critical "just finished OAuth, waiting to land in the app" moment —
exactly the interaction Shopify's own guidance says should feel instant
("merchants must be redirected straight to the app UI").

**Fix:** either the declarative approach in W4 below (which removes this entirely), or if
staying imperative, defer registration to a background task and redirect the merchant
immediately.

### W4 — Imperative webhook registration where declarative (TOML) is now the recommended pattern

**Where:** the whole `register_webhooks()` design.

The order/checkout/app-uninstalled topic set is fixed and known at build time — it never
varies per merchant. For exactly this case, Shopify's current-recommended pattern is
declaring `[[webhooks.subscriptions]]` blocks in `shopify.app.toml` (the same mechanism
this migration already uses for the compliance topics, per the commented-out block in
`shopify.app.toml`) — Shopify then handles subscribe/unsubscribe automatically on every
install/uninstall, with zero application code. This would eliminate `register_webhooks()`
entirely, remove W1 and W3 above as a side effect, and remove the "OAuth save succeeded
but webhook registration silently partially failed" class of bug altogether.

This wasn't wrong when built — the TOML managed-webhooks feature's exact current
CLI-version support was genuinely uncertain at the time (flagged then), and the imperative
approach is the safe fallback. Worth revisiting once the extension is actually deployed
and the CLI's real behavior is confirmed.

### W5 — PII in application logs from the compliance webhook handler

**Where:** `webhooks.py`, `handle_shopify_compliance_webhook()`, the `logger.info` call
that includes `payload={json.dumps(payload)[:2000]}`.

`customers/redact`/`customers/data_request` payloads contain the customer's email, name,
and potentially order history — this logs up to 2000 characters of that directly into
application logs (Railway logs, presumably less access-controlled and differently-retained
than the DB). There's a mild irony in a GDPR-compliance handler being the thing that writes
customer PII somewhere with less oversight than the primary datastore.

**Fix:** log the resolved `client_id`/`topic`/`shop_domain` for the audit trail (already
useful, low-risk), but not the full payload — or route full-payload logging to a
storage location with the same access/retention controls as the rest of PII in this
system.

### W6 — No rate limiting on the OAuth install/callback routes

**Where:** `app.py`, `connect_shopify_install`, `connect_shopify_callback`.

Every other sensitive-ish route pattern I saw in this codebase (`/chat`, lead submission)
has `limiter.limit(...)` applied. These two don't. The callback does meaningful work even
on a failed attempt (session lookups, potentially a token-exchange HTTP call before HMAC
fails), and the install route can be hit repeatedly with arbitrary `shop` values.
Low-severity on its own — Shopify's own HMAC check on the callback bounds what an attacker
can actually accomplish — but it's an inconsistency with the rest of the app's posture
worth closing.

---

## 🟡 Recommendations

### R1 — No idempotency guard against duplicate webhook *processing time*, only duplicate *data*

`_upsert_order`'s `ON CONFLICT DO UPDATE` correctly makes duplicate delivery idempotent
from a data-correctness standpoint (verified — this is solid, see ✅ below). What's not
addressed: Shopify's QA guidance calls for webhooks to be **queued and processed
asynchronously**, returning 200 immediately, rather than doing the DB work synchronously
inside the request. Current handlers do everything inline. For a DB hiccup or a slow
query, that risks exceeding Shopify's webhook timeout window, which reads as a failure and
triggers a retry — not incorrect, just not the recommended pattern, and worth reconsidering
if webhook volume grows.

### R2 — Billing model, if public listing is ever the goal

Not a code issue — flagging because it'll block a submission and is better known now than
discovered at submission time. Lumvi charges via Flutterwave. Shopify requires the Billing
API for any charges collected through an app going through App Store review. This is a
business/pricing decision, not something fixable in this codebase.

### R3 — Extension TOML files still unverified against a live CLI scaffold

Repeating this from Phase 3 because it's still true and easy to forget by the time deploy
day arrives: `shopify.app.toml` and the extension's `shopify.extension.toml` were written
from documentation, not generated or confirmed against a real `shopify app` CLI project.
Reconcile before deploying, not after something breaks.

### R4 — `app/uninstalled` deactivates but never expires the stored token

`delete_integration` sets `is_active=FALSE` — correct, reversible, matches the dashboard's
manual disconnect behavior. But the encrypted access token itself stays in
`platform_config` indefinitely (Shopify has already revoked it on their end at uninstall,
so it's not independently exploitable, but it's also not *gone* — which matters for
`shop/redact`, which fires 48 hours after uninstall (corrected here — an earlier
version of this report said 48 days, verified against Shopify's own compliance
docs while implementing the B3 fix) and is supposed to actually erase it and currently
doesn't — see B3).

### R5 — Design/Polaris and embedding, only relevant for public listing

Non-embedded, non-Polaris — a deliberate, previously-flagged Phase 3 decision, correct for
a direct-install model. Repeating here only so it's visible in one place alongside the
other public-listing-only items (B2, B3, B4, R2) rather than scattered across the
migration's history.

---

## ✅ Verified sound (checked directly for this report, not assumed)

- **HMAC comparison is timing-safe** — `_verify_shopify_signature` uses
  `hmac.compare_digest`, not `==`. Pre-existing code, unaffected by this migration.
- **Order upserts are genuinely idempotent** — `INSERT ... ON CONFLICT (client_id,
  order_id) DO UPDATE`, correctly handles Shopify's at-least-once webhook delivery without
  duplicate rows.
- **Session cookies are correctly hardened** — `SESSION_COOKIE_SECURE=True`,
  `HTTPONLY=True`, `SAMESITE='Lax'`. Pre-existing, unaffected by this migration.
- **OAuth CSRF protection (`state`) and callback HMAC verification** are both implemented
  correctly — verified against Shopify's documented algorithm (`urlencode(sorted(...))`),
  not a naive string join.
- **Encrypted-at-rest token storage** (Phase 0) — round-trip and legacy-plaintext
  fallback both verified via `test_pure_logic.py`, 18/18 passing.
- **SQL injection surface** — every query touched in this migration uses parameterized
  `%s` placeholders; none of the new code interpolates user input into SQL strings.
- **OAuth scope minimization** — `read_products,read_orders` only, no write scopes
  requested anywhere.
- **Multi-tenant client targeting (the agency-selector fix)** — `verify_client_ownership`
  correctly gates the explicit-`client_id` install path; re-verified this still holds
  after the compliance-webhook and integration-cap changes added since.
- **Liquid injection surface in the theme embed** — `{{ shop.permanent_domain }}`
  interpolated into a `<script>` tag, but it's a Shopify-controlled value (always
  `xxx.myshopify.com`), never merchant-supplied free text — low risk, reviewed rather than
  assumed safe.

---

## Suggested fix order

1. **B1** (cross-tenant webhook replay) — before any real merchant traffic touches this,
   direct-install or not. This is the one that matters regardless of App Store plans.
2. **B3** (compliance webhooks) — decide the actual redaction scope; this is a real
   compliance question independent of whether you ever submit for public listing.
3. **W1/W3/W4 together** — the webhook-registration redesign addresses three findings at
   once and is worth doing as one piece of work, not three separate patches.
4. **B2, B4, R2, R5** — bundle these for whenever public App Store listing becomes an
   actual near-term goal; no reason to do them before then.
5. **W2, W5, W6, R1, R4** — opportunistic, whenever touching the relevant files next.
