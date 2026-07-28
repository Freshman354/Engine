# Phase 5 — Test Plan (Shopify native app migration)

## What's already verified vs. what needs you

**Already run, against the real code, in a sandbox with no DB/network access** — two suites:

- `test_pure_logic.py` — `crypto_utils.py` + the encryption/HMAC layer of `webhooks.py`.
  Encryption round-trips, legacy-plaintext backward compatibility, both HMAC verification
  functions. **18/18 passed.**
- `test_webhook_routing.py` — `handle_shopify_webhook()` and
  `handle_shopify_compliance_webhook()` themselves. Only DB-writing collaborators are
  mocked (`_upsert_order`, `delete_integration`, `models.get_client_by_id`,
  `models.upsert_abandoned_cart`) — signature verification, topic matching, and JSON
  parsing all run as real, unmodified code. This is what actually confirms the
  `orders/created` vs `orders/create` fix — the test asserts the old string is correctly
  treated as unrecognized, not just that the new one works. **17/17 passed** (one bug
  found along the way — in the test itself, a stale signature reused across two different
  payloads; caught by the assertion failing, fixed, reran).

That's 35 real, executed tests, run against your actual files, not reimplementations.
**What this can't do:** confirm your real SQL is correct against real Postgres — mocking
stops at the DB-API boundary on purpose (see the top-of-file comment in
`test_webhook_routing.py` for exactly why SQLite wasn't a viable stand-in: the queries use
JSONB operators, `ON CONFLICT ON CONSTRAINT`, and `NOW()`, none of which SQLite supports
without rewriting the SQL — which would mean testing different queries than the real ones).

**Needs a real dev store, a deployed instance, and the Partner Dashboard app configured.**
I can't do any of this from here — no network access to Shopify or your DB, and per your
instruction, I'm not going to improvise what "should" happen. Everything below is a
checklist for you to run, with exactly what to look for at each step.

## Prerequisites

- [ ] `SHOPIFY_APP_CLIENT_ID` / `SHOPIFY_APP_CLIENT_SECRET` set in Railway, matching a real
      Partner Dashboard app
- [ ] `INTEGRATION_ENCRYPTION_KEY` confirmed present (flagged back in Phase 0)
- [ ] Redirect URL in Partner Dashboard exactly matches `https://<your-domain>/connect/shopify/callback`
- [ ] Theme app extension deployed via `shopify app deploy` (Phase 3's files were a draft,
      not CLI-verified — reconcile against the generator output first)
- [ ] A Shopify dev store (Partner Dashboard → Stores → Add store → Development store)
- [ ] "Protected customer data access" requested in Partner Dashboard (orders/checkouts
      topics won't deliver real data without this — registration will still succeed, per
      the note in `commerce_adapters.py`)

---

## Scenario 1 — Fresh headless install (no prior Lumvi account)

This is the core new flow — merchant finds/installs the app from Shopify with zero prior
relationship to Lumvi.

**Steps:**
1. From the dev store's admin → Apps → find your app (or use a direct install link) → Install
2. Approve the requested scopes on Shopify's consent screen

**Expected:**
- Redirected to Lumvi's onboarding wizard, already at step 2 ("Importing your store"),
  **already logged in** — no signup form, no password step
- Wizard completes normally through to step 5
- Step 5 shows the gold "One last step — activate on your store" banner
- Clicking it opens the theme editor with the embed block pre-toggled; clicking Save
  in Shopify's editor and visiting the storefront shows the chat widget

**Check in logs (Railway):**
- `[Shopify OAuth] connected shop=... client=... user=... headless=True`
- `[Shopify OAuth] webhooks registered shop=...` (or a `failed_topics=[...]` warning — note
  which topics, if any)

**Check in DB:**
- New row in `users` — email should be the store's actual contact email (or a
  `shopify-XXXXXX@installs.lumvi.net` placeholder if Shopify's shop.json had none)
- New row in `clients`, `user_id` pointing at that user
- New row in `client_integrations`, `platform='shopify'`, `is_active=TRUE`
- `platform_config->>'access_token'` should **not** be readable as plaintext — should look
  like a Fernet token (long base64-ish string with `gAAAAA...` prefix), not `shpat_...`
- `webhook_secret` column should equal your `SHOPIFY_APP_CLIENT_SECRET`, not a random hex string

---

## Scenario 2 — Dashboard-initiated connect (existing logged-in user, own store)

**Steps:**
1. Log into an existing Lumvi account with no Shopify connected yet
2. From the dashboard, click "Connect Shopify" → complete OAuth on the dev store

**Expected:**
- Redirected back to the dashboard (not onboarding)
- Success toast, activation banner appears with a working link
- The store card's platform badge shows Shopify correctly (this was the badge-defaulting
  bug fixed alongside Phase 4 — worth specifically confirming a WooCommerce-only account
  doesn't mislabel, and a brand-new unconnected client shows "Not connected", not "Shopify")

---

## Scenario 3 — integrations.html, targeting a specific client

Only relevant if you still have any multi-client (legacy agency-model) accounts.

**Steps:**
1. Log in as a user with 2+ clients under `get_user_clients`
2. On `/integrations`, select client B (not the first one) from the dropdown
3. Click "Connect Shopify" on client B's card

**Expected:**
- OAuth completes, lands back on `/integrations?client_id=<client B's id>`
- Client B's card shows connected — **client A must not be affected**

**Regression check (this is the bug from earlier this session):**
- Confirm in the DB that the `client_integrations` row was written against client B's
  `client_id`, not client A's. This was broken before the `client_id`-aware fix — worth
  explicitly re-verifying since it's easy to silently regress.

**Also test the rejection path:** manually hit
`/connect/shopify/install?client_id=<some client_id you don't own>&return_to=integrations`
while logged in — should redirect back with "That client wasn't found on your account,"
not silently connect to the wrong thing.

---

## Scenario 4 — Reinstall of a known/previously-uninstalled shop

**Steps:**
1. Complete Scenario 1 (fresh install)
2. Uninstall the app from the Shopify dev store admin
3. Re-install it

**Expected:**
- Step 3 lands the merchant straight back into their **existing** account (not a duplicate)
- No new row in `users` or `clients` — same `client_id` as before, integration reactivated

**Check in DB:**
- Still only one `users` row for that email, one `clients` row for that shop
- The `client_integrations` row's `is_active` flips FALSE → TRUE across the
  uninstall/reinstall cycle, same row (check `created_at` doesn't change, `updated_at` does)

---

## Scenario 5 — Uninstall handling

**Steps:**
1. With an active connection, uninstall the app from Shopify admin

**Expected:**
- `app/uninstalled` webhook arrives at `/webhooks/shopify/<client_id>`
- Chat widget stops responding to order/inventory queries for that store shortly after
  (existing token still technically valid at Shopify's end until revoked, but Lumvi should
  treat the integration as inactive)

**Check in logs:**
- `[Shopify] app/uninstalled client=... deactivated=True`

**Check in DB:**
- `client_integrations` row: `is_active=FALSE`

---

## Scenario 6 — Webhook delivery (the actual point of Phase 2)

*(Topic-matching and signature-verification logic itself is already covered by
`test_webhook_routing.py` — this scenario is about confirming Shopify actually delivers
and your real DB actually persists, not re-checking the routing logic.)*

**Steps:**
1. With an active connection and "Protected customer data access" approved, place a test
   order on the dev store

**Expected / check in DB:**
- `webhook_log` gets a new row: `platform='shopify'`, `event_type='orders/create'`,
  `status='ok'` — **this specific topic string is worth double-checking**; it's the bug
  found and fixed this migration (was `orders/created`, doesn't match what Shopify
  actually sends)
- The order data itself lands wherever `_upsert_order` writes it

**If nothing arrives:** check Partner Dashboard → your app → check whether "Protected
customer data access" is actually approved yet (this can take time / require justification
text) — registration succeeding doesn't mean delivery is unblocked.

---

## Scenario 7 — Compliance webhooks

These need the Partner Dashboard's "Compliance webhooks" URL actually pointed at
`/webhooks/shopify/compliance` first (see Prerequisites and the TOML file's comments).

**Steps:**
1. Partner Dashboard has a "Send test notification" option for each compliance topic, or
   you can `curl` the endpoint directly with a hand-computed HMAC signed with
   `SHOPIFY_APP_CLIENT_SECRET`

**Expected:**
- 200 response
- Log line: `[Shopify Compliance] <topic> shop=... client=...`
- A `webhook_log` row if `client_id` resolved

**Remember:** this endpoint logs and returns 200 — it does **not** perform actual data
export/deletion (flagged deliberately when built). If you're relying on this for real GDPR
compliance obligations, the follow-up (deciding what "redact" actually purges from
conversations/leads) still needs to happen before you can say these are fully handled, not
just acknowledged.

---

## Scenario 8 — Plan cap enforcement

**Steps:**
1. On a free/starter test account (`integrations_limit=1`), connect one integration
   (Acuity manually, or anything)
2. Attempt to connect Shopify via OAuth on the same client

**Expected:**
- Redirected back with "Your plan allows up to 1 connected integration(s). Upgrade to
  connect more." — **not** a silent success
1. Then **uninstall and reinstall Shopify on that same client** (with Acuity still connected)
2. Confirm reinstalling Shopify does **not** get blocked by its own prior (now-reactivating)
   connection — this was the specific edge case the fix was designed around

---

## Scenario 9 — Theme embed activation

**Steps:**
1. After any successful connect, click the "Activate on my store" link
2. In the theme editor, click Save
3. Visit the storefront

**Expected:**
- Chat widget appears on the storefront with no code visible anywhere in the theme's
  source (view-source should show a `<script data-client-id>` tag injected by the embed
  block's bootstrap script, not something a merchant typed in)
- Confirm it still works if you **don't** click through the activation link — i.e. test
  that a merchant who ignores the banner and finds the toggle manually via
  Theme Settings → App embeds gets the same result

---

## Known gaps going into this (not blockers, just things to know before you start)

- `list_integrations()`'s docstring says "active integrations" but doesn't filter
  `is_active` in the actual query — the plan-cap fix worked around this for the OAuth path,
  but `create_platform_integration` (the legacy manual endpoint) has the same latent bug,
  unfixed, flagged last turn
- Compliance webhooks log and acknowledge only — no automated redaction yet
- Extension TOML files are a documented-but-unverified draft — reconcile against
  `shopify app generate` output before relying on them
- App is non-embedded by design (Phase 6, App Bridge, was explicitly deferred) — if you
  submit for public App Store listing, Shopify's review may flag this
