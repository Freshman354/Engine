# Follow-up Production Readiness Audit — Round 2

**Scope:** only the code that changed to fix B1, B3, W1, W3, W4 from the original
report. Everything previously marked ✅ Verified sound is out of scope here (still
holds — nothing in this round touched it). Written as an independent reviewer of the
fixes, not as their author explaining them.

**Read this first:** three real bugs were found and fixed *during this round*, before
being written up here — not by review, but by actually running the tests against the
real code. That's worth sitting with. A code review of the W1 diff would very likely
have looked correct; the retry loop, the backoff math, the Retry-After handling were
all sound in isolation. What review wouldn't have caught is that the refactor split
`register_webhooks()` into two methods and left `import requests` in the one that no
longer uses it — a `NameError` that would have crashed every single webhook
registration attempt in production, silently, since it's wrapped in a try/except that
logs and returns `False` rather than propagating. `py_compile` doesn't catch it either
— it's syntactically valid Python. Only executing it did.

---

## Summary

| Severity | Count |
|---|---|
| 🔴 Blocker (found and already fixed pre-delivery) | 3 |
| 🟠 Warning | 5 |
| 🟡 Recommendation | 4 |
| ✅ Verified sound | 6 |

---

## 🔴 Blockers — found and fixed before this reached you

Listed as blockers because of what they *would* have been if shipped, not because
they're still open. Included so the fix is on record, not just the current clean
state.

### RB1 — `NameError` in `_register_one_webhook_topic`, crashes every registration attempt

Covered above. Root cause: a local `import requests` lived in `register_webhooks()`;
the W1 refactor moved the code that uses it into a new sibling method,
`_register_one_webhook_topic`, without moving (or duplicating) the import. Python
local imports are scoped to the function they're written in — a sibling method has no
access to it. Fixed by adding the import to the method that actually uses it, and
removing the now-dead one from the caller. Caught by `test_register_webhooks.py`'s
first assertion — the happy-path test failed immediately with the `NameError`.

**Why this matters beyond "it's fixed now":** it's a concrete demonstration that
`py_compile` and even careful code reading are not sufficient verification for
anything involving a refactor that moves code between scopes. Worth keeping in mind
for any future change to this file.

### RB2 — `models.delete_client()`'s return value was about to be trusted incorrectly

`delete_client()` returns `None` implicitly on success and re-raises on failure — it
has no `return True`/`return False` anywhere. The first draft of
`process_due_shopify_shop_redactions()` wrote `client_ok = models.delete_client(client_id)`
and branched on `client_ok` — which would have been `None` (falsy) on every single
success, meaning every successful shop/redact deletion would have been logged and
recorded as **failed**, and — worse — because `hard_delete_shopify_integration` was
still called unconditionally in that draft, the client's data would actually be gone
while the audit trail says the redaction failed. Caught before writing the test, by
re-reading `delete_client`'s actual source rather than assuming a return-boolean
convention from its sibling function `toggle_client_suspended` (which *does* return a
boolean, three lines below it in the same file — an easy assumption to make by
proximity). Fixed by wrapping the call in its own try/except and treating "no
exception" as success.

### RB3 — a factual error in the original report (48 days vs. 48 hours)

The original production-readiness-report.md stated `shop/redact` fires 48 days after
uninstall. Verified against Shopify's own compliance documentation while implementing
B3: it's 48 **hours**. Corrected in that file. Doesn't change any of the engineering
decisions made around it (the internal grace period before this codebase actually
executes the deletion is still a separate, shorter window — see W-equivalent findings
below), but worth knowing the original number was wrong if it informed any planning
conversations already.

---

## 🟠 Warnings

### RW1 — no index on the new `orders` query pattern

`_find_orders_by_email` / `_redact_orders_by_email` (new this round) query `orders` by
`client_id + customer_email` — a filter pattern that didn't exist before this fix
(orders were previously only ever looked up by `order_id`). Whether there's a
supporting index on `(client_id, customer_email)` wasn't verified — couldn't be, no DB
access from this sandbox. If there isn't one, both a `customers/data_request` and a
`customers/redact` degrade to a full scan of that client's order history as it grows.
Worth checking directly against the real schema, not assumed either way.

### RW2 — `customers/redact` only covers `orders`, not conversations/leads

Named honestly in the code's own docstrings, but worth surfacing here explicitly
rather than leaving it implicit: if a customer's email ever appears in a chat
conversation or a lead record independent of an actual order (plausible — a shopper
could type their email into the chat widget without ever placing an order), that data
is untouched by `customers/redact`. `shop/redact` doesn't have this gap (it goes
through `delete_client`, which covers conversations and leads), but the
narrower, faster, customer-scoped topic does. This is a real scope boundary of what
was built, bounded by which tables' schemas were actually verified (`orders`) versus
guessed at (conversations/leads schemas were never seen). Closing it needs those
files, not more inference from this one.

### RW3 — `process_due_shopify_shop_redactions` is a single synchronous loop

If many shop/redact requests become due in the same cron run (a burst of uninstalls,
unlikely but possible), they're processed sequentially in one HTTP request handler —
each `delete_client()` call alone runs ~11 sequential DELETE statements. Not a risk to
Shopify's webhook timeout (this runs in the cron job, decoupled from the webhook
response, which is the whole point of the scheduling design) — the risk is the cron
HTTP request itself running long enough to hit a platform-level request timeout
(Railway or its load balancer), which would leave the remaining due rows unprocessed
until the next scheduled run. Low likelihood at any realistic volume; worth a batch
size cap if that ever changes.

### RW4 — duplicate webhook delivery creates duplicate audit rows, not duplicate bad effects — but this is incidental, not by design

Shopify redelivers webhooks (documented at-least-once delivery). Neither
`_record_compliance_request` nor the new orders-lookup functions check Shopify's
`X-Shopify-Event-Id` header for deduplication. Traced through what actually happens on
a redelivered `shop/redact`: a second `'scheduled'` row gets inserted for the same
client, and the cron job would process both — but `delete_client()`'s DELETE
statements simply match zero rows the second time (already gone), so it happens to be
harmless. That's idempotent by the accident of DELETE-on-nothing being a no-op, not
because anything was designed to dedup. A `customers/redact` retry is similarly
harmless by the same accident. Worth adding real `X-Shopify-Event-Id`-based dedup at
some point — not because anything is currently broken, but because relying on an
implicit property of DELETE statements to stay correct is fragile if this code changes
later.

### RW5 — the compliance TOML block is now live, still unverified

The `[[webhooks.subscriptions]]` block for `customers/data_request` /
`customers/redact` / `shop/redact` was uncommented this round, now that the handler
actually does something with them. It was written from Shopify's documentation, same
as before, still never confirmed against a real `shopify app config push`. The
difference from before: previously this was inert (commented out), so an error in it
had no consequence. Now it's live config — if the syntax is wrong, the CLI push itself
should catch it, but that's the first real test it will get.

---

## 🟡 Recommendations

### RR1 — the daemon thread has no persistence across a process restart

If the app process restarts (deploy, crash) while `_register_shopify_webhooks_background`
is mid-run, that specific attempt is simply lost — no record, no automatic retry. Given
the TOML-declarative subscriptions (W4) now cover the same topics as a parallel
mechanism, this is a real but low-severity gap: a merchant who happens to install during
that exact window still gets webhook coverage via the TOML path even if the imperative
background attempt was interrupted. Worth knowing this is *why* running both mechanisms
together (rather than picking one) is actually load-bearing, not redundant for its own
sake.

### RR2 — the email delivery path is unverified end-to-end

`_get_client_owner_email` → `mail.send()` in the `customers/data_request` handler was
tested with the *orchestration* mocked correctly (mail called or not, based on whether
orders were found) — but never against a real database (to confirm
`get_client_by_id`/`get_user_by_id` actually resolve correctly) or a real SMTP
configuration (`flask_mail` isn't installable in this sandbox at all — see the test
file's own note on injecting a fake module just to exercise the code path). Worth a
real end-to-end test — trigger a real `customers/data_request` against a dev store —
before relying on this for an actual compliance deadline.

### RR3 — no test coverage for `migrate_integrations()`'s new CREATE TABLE statements

The `shopify_compliance_requests` table and its two indexes were added to the existing
migration function. Syntax was checked by eye against the existing `client_integrations`
CREATE TABLE in the same function (consistent style, `IF NOT EXISTS` idempotency), but
never run against a real Postgres instance — same sandbox limitation as everything
else DB-related in this project. First real deploy is the first real test of this DDL.

### RR4 — `SHOPIFY_SHOP_REDACT_GRACE_DAYS` and `ACCOUNT_DELETION_GRACE_DAYS` now live in
different files with no shared pattern

`ACCOUNT_DELETION_GRACE_DAYS` lives in `constants.py` (a file never uploaded to this
conversation, so never directly edited). The new `SHOPIFY_SHOP_REDACT_GRACE_DAYS` was
added as a plain module-level constant in `webhooks.py` instead, specifically to avoid
guessing at `constants.py`'s structure without having seen it. Purely a consistency
nit — worth moving into `constants.py` alongside its sibling if/when that file is
shared, not urgent enough to block anything.

---

## ✅ Verified sound (re-checked fresh for this round)

- **B1's shop-domain cross-check applies uniformly across every topic** — explicitly
  re-tested against `app/uninstalled` specifically (not just `orders/create`), since
  the check sits before topic routing branches and it would have been easy to
  accidentally place it somewhere only some topics pass through.
- **The compliance endpoint was re-confirmed to not need the same fix** — it resolves
  `client_id` *from* the HMAC-verified payload's `shop_domain`, rather than trusting a
  URL-supplied one, so there's no equivalent spoofing surface to close there. Reasoned
  through fresh, not just carried over from the original design intent.
  the shop-domain from the payload, and payload is trustworthy only after the HMAC
  over the exact raw bytes has already verified — sound.
- **`_redact_orders_by_email` and `_find_orders_by_email` use parameterized queries
  throughout** — no new SQL injection surface introduced.
- **`register_webhooks()`'s retry logic correctly distinguishes failure types** —
  429 retries with backoff; a genuine `userError` (e.g. malformed URL) fails
  immediately without wasting retries on something retrying won't fix. Verified via
  `test_register_webhooks.py`'s explicit test for exactly this distinction.
- **The background thread is provably safe to run without Flask context** — confirmed
  by re-reading `register_webhooks()`'s full call chain: no `models.get_db()`, no
  `current_app`, no session access anywhere in it — pure outbound HTTP.
- **`hard_delete_shopify_integration` closes the exact gap it was built for** —
  re-confirmed `client_integrations` has no FK to `clients` (checked directly in the
  DDL, not assumed) and isn't in `delete_client`'s table list, so without this function
  a shop/redact would silently leave the encrypted access token behind. With it, both
  tables it's responsible for (`client_integrations`, `webhook_log`) are covered.

---

## What this round didn't touch, and shouldn't be assumed fixed

B2 (REST-vs-GraphQL), B4 (manual shop-domain entry), R2 (Billing API), R5
(embedding/Polaris) — all explicitly out of scope per this round's instructions, and
none of this round's changes affect them either way. Still exactly where the original
report left them.
