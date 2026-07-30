# Lumvi → Shopify Native App Migration — File Package

This contains every file I created or modified across Phases 0–5. It does **not**
contain your whole codebase — only what changed. Anything not listed below
(`tools.py`, `ai_helper.py`, `shopify_connect.py`, `chat.html`, `widget.js`, every
other blueprint, `crypto_utils.py`, etc.) was read but never touched — leave those
exactly as they are.

## Where each file goes

Paths below are relative to your project root — match them against your actual
layout; these are what your own imports/`render_template()` calls implied
throughout this migration, not guesses.

```
app.py                               → project root (replaces your current app.py)
webhooks.py                          → project root
commerce_adapters.py                 → project root
crypto_utils.py                      → NOT a deliverable — unmodified, included only
                                        so testing/ runs out-of-the-box; skip this one
models/__init__.py                  → models/
models/users.py                     → models/
models/clients.py                   → models/ (new this round — the delete_client
                                       orders-table fix, see above)
blueprints/auth.py                  → blueprints/
blueprints/cron.py                  → blueprints/ (new this round — adds the
                                       /cron/shopify-redactions job for B3's
                                       delayed shop/redact processing)
templates/dashboard_enterprise.html  → wherever render_template('dashboard_enterprise.html')
                                        looks (this is your actual template name —
                                        confirmed from blueprints/auth.py's own
                                        render_template() call, not renamed on my end)
templates/onboarding.html           → same, for onboarding.html
templates/integrations.html         → same, for integrations.html
shopify-extension/                  → a SEPARATE Shopify CLI project — see below,
                                       this is NOT part of your Flask app
testing/                             → wherever you keep test scripts, not deployed
  test_pure_logic.py                   18 tests: encryption, HMAC verification
  test_webhook_routing.py              41 tests: webhook topic routing, B1 shop-domain
                                        cross-check, B3 compliance handling, both handlers
  test_register_webhooks.py            12 tests: W1 retry/backoff — this is the suite
                                        that caught the NameError bug, see the follow-up report
  models_stub.py                       rename to models.py before running any test
  utils_stub.py                        rename to utils.py before running any test —
                                        only needed for test_register_webhooks.py
                                        (commerce_adapters.py imports utils.get_logger)
  phase5-test-plan.md                  manual dev-store checklist (needs live Shopify/DB)
diffs/                              → not deployed — reference only, one .diff per
                                       modified file, against exactly what you
                                       originally uploaded in this conversation
```

## `shopify-extension/` is not a Flask thing

This is a separate project the Shopify CLI manages:
```
shopify-extension/
├── shopify.app.toml
└── extensions/lumvi-chat-embed/
    ├── shopify.extension.toml
    └── blocks/chat-embed.liquid
```
I flagged this clearly back in Phase 3 and it's still true: I can't run the Shopify
CLI or touch a live Partner Dashboard app from here, so these are a well-informed
draft, not a CLI-verified scaffold. Before deploying: create the app in your Partner
Dashboard, run `shopify app config link` in a real CLI project, and reconcile these
files against what that generates — don't push them blind.

## Read this first

`production-readiness-report.md` — the original audit.
`followup-production-readiness-report.md` — covers the B1/B3/W1/W3/W4 fixes applied
on top of the original report, written as an independent re-review of those specific
changes. It documents three real bugs found and fixed *during* that round, including
one (a `NameError` that would have crashed every webhook registration attempt) that
only running the tests caught — code review alone would very likely have missed it.
`privacy-policy-draft-addition.md` — **new this round**, read this if you're working
through the Shopify Partner Dashboard rejection. Not legal advice, have an actual
lawyer review it.

### What changed in the B1/B3/W1/W3/W4 round

- **B1 fixed** — `webhooks.py` now cross-checks Shopify's `X-Shopify-Shop-Domain`
  header against the client's registered shop before processing any webhook,
  closing a cross-tenant replay gap.
- **B3 fixed** — `customers/data_request`, `customers/redact`, `shop/redact` now
  actually do something (compile/redact order data, schedule full client deletion)
  instead of verifying, logging, and returning 200. New table
  (`shopify_compliance_requests`), new cron job (`blueprints/cron.py`:
  `/cron/shopify-redactions`), new functions in `webhooks.py`.
- **W1/W3/W4 addressed** — `register_webhooks()` now retries on 429 with backoff
  (honoring Shopify's `Retry-After` header); registration runs in a background
  thread instead of blocking the OAuth redirect; the fixed topic set is now also
  declared in `shopify.app.toml` as the primary mechanism, with the imperative call
  kept as a self-healing backup (the file explains why order/checkout topics
  couldn't fully move to declarative-only — a real URL-scheme conflict with
  per-client routing, not an oversight).

### What changed in this round (Shopify Protected Customer Data remediation)

Built in direct response to a Partner Dashboard rejection — the Data Protection
Details questionnaire was answered "No" to both "do you have retention periods"
and "do you log access to personal data," because neither system existed.

- **Retention, tied to account lifecycle** (your choice) — `app/uninstalled` now
  schedules a full data purge 30 days after disconnect (`SHOPIFY_UNINSTALL_
  RETENTION_GRACE_DAYS` in `webhooks.py`), independent of whether Shopify's own
  separate `shop/redact` webhook is even configured.
- **A real gap found while building that**: `models/clients.py`'s `delete_client()`
  — used by both this new retention system and Lumvi's existing self-service
  account deletion — never actually deleted the `orders` table. Customer order
  data (email, name) survived every account deletion until now. Fixed directly in
  `delete_client()`, not just the Shopify-specific path, since this affects any
  account deletion, not only Shopify-connected ones.
- **Access logging** — new `personal_data_access_log` table (`webhooks.py`), logs
  every read of Shopify customer order/checkout data (webhook sync, live lookup
  compliance requests) with a hashed reference, not the raw PII itself, so the audit
  log doesn't become a second copy of what it's protecting.
- **Privacy policy** — a draft addition, not yet merged into your actual policy.
  See `privacy-policy-draft-addition.md`.

**Still needed, not done here:** logging access on the *live* GraphQL order-lookup
path (`commerce_adapters.py`'s `get_order`, called from wherever the AI actually
invokes it in `tools.py`/`ai_helper.py`) — those files weren't available when this
was built. The webhook-sync path is fully covered; the live-lookup path isn't yet.

## Suggested deploy order

1. **Env vars first** — `SHOPIFY_APP_CLIENT_ID`, `SHOPIFY_APP_CLIENT_SECRET`,
   confirm `INTEGRATION_ENCRYPTION_KEY` is actually set (flagged in Phase 0)
2. **`models/__init__.py`, `models/users.py`** — no external dependencies on
   anything else here
3. **`webhooks.py`, `commerce_adapters.py`** — depend on the models changes above
4. **`app.py`** — depends on all of the above
5. **`blueprints/auth.py`** — the `client.platform` dashboard fix, independent of
   the rest
6. **Templates** — cosmetic/UX layer, safe to deploy anytime after `app.py` (they
   reference query params `app.py` now generates)
7. **Shopify extension** — separate CLI deploy, whenever the Partner Dashboard app
   is ready

## Before you trust any of this in production

- Run the automated tests yourself — same 71 tests I ran across three suites
  (`test_pure_logic.py`, `test_webhook_routing.py`, `test_register_webhooks.py`),
  against your real files, but you should see them pass on your own machine too,
  not just take my word for it:
  ```
  cd testing/
  cp models_stub.py models.py   # stands in for the real DB-backed models package
  cp utils_stub.py utils.py     # stands in for utils.py (commerce_adapters.py's
                                  # logger import) — only test_register_webhooks.py
                                  # needs this one
                                  # both are just for this test run — see the comment
                                  # at the top of each test file for exactly what
                                  # they stub
  python3 test_pure_logic.py
  python3 test_webhook_routing.py
  python3 test_register_webhooks.py
  rm models.py utils.py          # remove them after — don't leave them shadowing
                                  # the real packages for anything else
  ```
  `crypto_utils.py` is included in this package *only* so these tests run
  out-of-the-box — it was never modified, don't treat it as a deliverable to deploy
  over your existing copy (though they should be identical either way).
- Work through `testing/phase5-test-plan.md` on a real dev store — this is the part
  I could not execute from this sandbox (no network access, no live Shopify or DB
  access), so it's genuinely unverified until you run it
- Read the `diffs/` folder before deploying — every diff is against exactly what
  you uploaded, so it's a precise record of what changed and why (the reasoning
  is in the code comments themselves, not just the diff)

## Known gaps, carried over from earlier phases (not fixed, deliberately)

- `list_integrations()`'s docstring says "active integrations" but its query
  doesn't actually filter `is_active` — `create_platform_integration` (legacy
  manual-connect endpoint) inherits this; the new OAuth path was written to avoid
  it, that endpoint wasn't touched
- Compliance webhooks (`customers/data_request`, `customers/redact`, `shop/redact`)
  verify, log, and return 200 — they don't perform actual data export/deletion yet
- Non-embedded app (no App Bridge) — fine for a direct install link, may get
  flagged in review if you submit for public App Store search-listing placement
