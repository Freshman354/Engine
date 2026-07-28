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
blueprints/auth.py                  → blueprints/
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
  test_webhook_routing.py              17 tests: webhook topic routing, both handlers
  models_stub.py                       rename to models.py before running either test
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

- Run the automated tests yourself — same 35 tests I ran (`test_pure_logic.py`,
  `test_webhook_routing.py`), against your real files, but you should see them pass
  on your own machine too, not just take my word for it:
  ```
  cd testing/
  cp models_stub.py models.py   # stands in for the real DB-backed models package
                                  # just for this test run — see the comment at the
                                  # top of each test file for exactly what it stubs
  python3 test_pure_logic.py
  python3 test_webhook_routing.py
  rm models.py                   # remove it after — don't leave it shadowing the
                                  # real models/ package for anything else
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
