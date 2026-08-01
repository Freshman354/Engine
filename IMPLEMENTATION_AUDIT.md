# Lumvi Admin Dashboard — Implementation Audit

Audited against the original requirements, verified directly against the delivered code (not against my own memory of what I intended to build). Every ✅/🟡/❌ below reflects what the code actually does, checked line-by-line for this report.

**Architecture confirmation up front:** everything below lives inside the single existing `admin_bp` Flask blueprint (`admin_routes.py`) and the single existing `admin_dashboard.html` template, gated by the same `@admin_required` decorator as every pre-existing admin route. No new blueprint, no new template, no separate admin page was created at any point.

---

## 1. User Activity Logging

| Event | Status |
|---|---|
| Signup | ✅ |
| Successful login | ✅ |
| Failed login | ✅ |
| Logout | ✅ |
| Shopify connected | ✅ |
| Shopify disconnected | ✅ |
| First AI conversation | ✅ |
| Subscription/plan change | ✅ |

### Signup — ✅
**Files:** `blueprints/auth.py` (lines 135–143, and 279–284 for the Google OAuth path), `models/billing.py` (`track_event`, line 172)
**How it works:** After a new user row is created and logged in, `auth.py` captures IP (`_client_ip()`, imported from `bot_protection.py`) and User-Agent, then calls `models.track_event('signup', user_id=user_id, metadata={'email': email, 'plan': intended_plan}, ip_address=_ip, user_agent=_ua)`, followed by `models.set_signup_ip(user_id, _ip)` to write the one-time `users.signup_ip` column. The Google OAuth callback does the same, but only when `create_or_link_google_user(..., return_is_new=True)` reports a genuinely new account — not on every returning-user Google login (see §7 for why that distinction mattered).
**Access from dashboard:** User detail modal → Account section (signup date) and Security & Activity section (signup IP); also feeds the Activity Timeline as the first entry.

### Successful login — ✅
**Files:** `blueprints/auth.py` (lines 330–333 for password login, 286–289 for Google)
**How it works:** On successful `verify_user()`, logs `track_event('login', ...)` with IP/UA, then calls `models.record_login(user_id)` which does `UPDATE users SET last_login_at = NOW(), login_count = login_count + 1`.
**Access from dashboard:** User detail modal → Account section shows "Last Login" and "Login Count" directly; also appears in the Activity Timeline.

### Failed login — ✅
**Files:** `blueprints/auth.py` (line 341)
**How it works:** This event didn't exist before this work at all. On invalid credentials, `models.track_event('failed_login', metadata={'email': email}, ip_address=_ip, user_agent=_ua)` fires. No `user_id` is attached (a failed attempt may not correspond to any real account), so it's matched back to a specific user later by email, not by ID — see `get_user_security_activity()` below.
**Access from dashboard:** User detail modal → Security & Activity section shows a "Failed Login Attempts" count for that user's email.

### Logout — ✅
**Files:** `blueprints/auth.py` (lines 348–353)
**How it works:** Didn't exist before. `track_event('logout', user_id=current_user.id, ip_address=..., user_agent=...)` fires at the top of the `/logout` route, before `logout_user()` clears the session.
**Access from dashboard:** Activity Timeline in the user detail modal.

### Shopify connected — ✅
**Files:** `webhooks.py` (root-level integration module, lines ~247–320, function `upsert_integration`)
**How it works:** Rather than adding a call at every place Shopify connection can happen (dashboard button, OAuth callback, re-auth), I hooked the single choke-point function both paths already call. `upsert_integration()` now checks whether the integration was already `is_active` *before* the upsert; if it wasn't (new connection or reconnect), it resolves the owning user via `models.get_client_owner_id(client_id)` and fires `track_event(f'{platform}_connected', ...)`. A plain credential/token refresh on an already-active integration does **not** re-fire the event.
**Access from dashboard:** User detail modal → Shopify section ("Store Connected: Yes", "Connection Date"); also in the Activity Timeline.

### Shopify disconnected — ✅
**Files:** `webhooks.py` (function `delete_integration`, lines ~540–570)
**How it works:** Same choke-point approach. `delete_integration()` is called both by the dashboard's manual "disconnect" action and by the `app/uninstalled` Shopify webhook handler — I edited the one shared function, so both paths are covered by a single change. It now only flips `is_active` when a row was actually active (`AND is_active = TRUE` added to the `WHERE` clause), and fires `track_event(f'{platform}_disconnected', ...)` only when that flip actually happened.
**Access from dashboard:** Activity Timeline; Shopify section shows "Installation Status: uninstalled".

### First AI conversation — ✅
**Files:** `blueprints/chat.py` (lines 65–107, inside `init_chat()`)
**How it works:** The chat blueprint has 7 different call sites for `log_conversation()` (demo mode, FAQ-only limit, human handoff, normal response, etc.). Instead of adding tracking logic to all 7, I wrapped the function once at dependency-injection time: `_log_conversation = _log_conversation_and_track_first`, a closure that calls the real logger, then checks `models.get_conversation_message_count(client_id) == 1`. If this was the first row ever for that client, it resolves the owning user via the already-injected `get_cached_client_owner(client_id)` and fires `track_event('first_ai_conversation', ...)`. No new table or counter — reuses the existing lifetime message count.
**Access from dashboard:** User detail modal → Activity Timeline; also directly powers the "1st AI Conversation" dashboard widget (§3).

### Subscription/plan changes — ✅
**Files:** `blueprints/billing.py` (lines 297–305 for upgrades via the browser-facing checkout return, 557–559 for cancellations)
**How it works:** `track_event('plan_upgrade', ...)` and `track_event('subscription_cancelled', ...)` already existed in this codebase before I touched it — I added IP/User-Agent capture to the two call sites that run inside an actual browser request (`current_user` is available). I deliberately **left the Flutterwave webhook call site (line ~455) unchanged** — that one fires from a server-to-server webhook with no real browser behind it, so attaching IP/UA there would record Flutterwave's server, not the customer, which would pollute the suspicion-scoring IP data.
**Access from dashboard:** Activity Timeline.

### Capture fields — Timestamp / IP / User-Agent / Metadata
**Files:** `models/migrations.py` (`migrate_admin_activity_tracking`), `models/billing.py` (`track_event`)
- **Timestamp:** ✅ — `analytics_events.created_at`, `DEFAULT CURRENT_TIMESTAMP`, pre-existing column.
- **IP address:** ✅ — new `analytics_events.ip_address TEXT` column, populated via `_client_ip()` (Cloudflare `CF-Connecting-IP` → `X-Forwarded-For` → `request.remote_addr` fallback chain, defined once in `bot_protection.py` as `get_client_ip()` and imported by both `auth.py` and `billing.py` rather than reimplemented per file).
- **User-Agent:** ✅ — new `analytics_events.user_agent TEXT` column, populated from `request.headers.get('User-Agent', '')`.
- **Metadata:** ✅ where applicable — stored as JSON text in the pre-existing `metadata` column (e.g. signup captures `{'email', 'plan'}`, plan_upgrade captures `{'plan', 'provider', 'cycle', 'amount', 'tx_ref'}`). Logout and failed_login have thinner metadata (email only, or none) since there's nothing more meaningful to attach.

---

## 2. Database

### `user_activity_log` table — ❌ **Not implemented as named — different design decision, flagging directly**
This table, under this name, does not exist anywhere in the delivered code. I did not build a new table for this. Instead, I extended the **existing** `analytics_events` table (which already had `user_id`, `event_name`, `metadata`, `created_at` before this work) with two new columns (`ip_address`, `user_agent`) rather than standing up a parallel logging table. This was a deliberate call under "reuse existing architecture, avoid duplicate code" — `analytics_events` was already the event log every part of this codebase wrote to (signup/login/password_reset already used it), so a second table would have meant two places to check for a user's history. Functionally this covers everything a `user_activity_log` table would have (event name, timestamp, IP, UA, metadata, queryable by user), just under the pre-existing table name. **If you specifically want a table literally named `user_activity_log`, that's a rename/migration I have not done — say so and I'll do it.**

### Required indexes — ✅
**File:** `models/migrations.py`, function `migrate_admin_activity_tracking`, lines 2392–2407
Four indexes added:
- `idx_analytics_events_user_created` on `(user_id, created_at DESC)` — powers the per-user activity timeline query
- `idx_analytics_events_ip_created` on `(ip_address, created_at) WHERE ip_address IS NOT NULL` — powers the IP-clustering suspicion signals
- `idx_analytics_events_name_created` on `(event_name, created_at)` — powers event-type aggregate counts (dashboard widgets, conversion rates)
- `idx_users_last_activity` on `(last_activity_at DESC)` — powers the Active 7d/30d dashboard widgets

### Migrations complete — ✅
**Files:** `models/migrations.py`, `models/__init__.py`, `admin_routes.py` (line 535), `app.py` (line 983)
The migration function is registered in **two** places, matching this codebase's existing pattern: the admin panel's manual "Run Migrations" button (`admin_routes.py`) for on-demand re-runs, **and** `app.py`'s `_optional_migrations` startup list, so it runs automatically on every deploy like every other migration in this codebase — not something that has to be remembered and clicked manually. (This second registration was missing in an earlier pass of this work and was added after you asked whether `app.py` had been touched — it hadn't been, and should have been; it is now.) The function itself uses `ADD COLUMN IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` throughout, so it's idempotent and safe to run repeatedly.

### User fields added — ✅
**File:** `models/migrations.py`, lines 2379–2382
- `users.last_login_at TIMESTAMP`
- `users.login_count INTEGER NOT NULL DEFAULT 0`
- `users.last_activity_at TIMESTAMP` — bumped by `track_event()` itself for **any** event type tied to a `user_id`, not just logins, so it reflects true last activity (Shopify actions, AI conversations, subscription changes, not just log-ins)
- `users.signup_ip TEXT` — write-once, via `WHERE signup_ip IS NULL` so it can never be overwritten after the fact

---

## 3. Admin Dashboard

### 3a. User Management

| Item | Status |
|---|---|
| User details | ✅ |
| Signup date | ✅ |
| Last login | ✅ |
| Login count | ✅ |
| Last activity | ✅ |
| Shopify connection status | ✅ |
| Email verification status (placeholder) | ❌ |
| Suspicion badge | 🟡 |

**Files (for the ✅ items above):** `admin_routes.py` (route `/admin/api/users/<int:user_id>/detail`, lines 437–491), `models/analytics.py` (`get_user_detail`, line 758), `templates/admin_dashboard.html` (modal markup ~line 781–797, rendering JS ~line 973–1080)
**How it works:** Clicking any row in the existing Users table (`onclick="openUserDetail({{ u.id }})"`, added directly to the `<tr>`) fires a single fetch to `/admin/api/users/<id>/detail`, which returns account info, Shopify status, security/activity data, and suspicion score in one JSON payload — one loading state instead of four separate calls. The modal reuses this dashboard's existing `.modal-overlay` / `classList.add('open')` pattern; I added one new CSS variant (`.modal-lg`) for the larger content, nothing structurally new.
**Access from dashboard:** Users tab → click any row.

**Email verification status (placeholder) — ❌, and I want to flag this precisely rather than let it slide:** Earlier in this project you were asked whether to (a) build the full flow, (b) add placeholder plumbing only, or (c) skip entirely — you chose **skip entirely**. Nothing was built: no `email_verified` column, no verification-related event, no placeholder field in the detail modal. I've now re-verified this directly (`grep -rln "email_verified" .` across the whole delivered codebase returns nothing). Your audit request above assumes placeholder plumbing exists — it does not, because "skip entirely" was the option chosen at the time. If you want the placeholder now, that's a small addition; see §6 for exactly what "placeholder" would mean.

**Suspicion badge — 🟡, and this is the most important gap in this report:** The suspicion score, level, and triggered signals **are** fully computed and **are** displayed — but only inside the per-user detail modal (`#udSuspicionBadge`, top-right of the modal header). There is **no badge, color, or indicator on the Users table itself**. This means you cannot currently scan the list and spot high-suspicion accounts — you have to open each user individually to see their badge. This was the intended job of Component 6, which covers Users-table columns, and it was never built. This is detailed fully in §3d and §8.

### 3b. Activity

| Item | Status |
|---|---|
| User activity timeline | ✅ |
| Login history | 🟡 |
| IP address history | ✅ |
| User-Agent history | ✅ |

**Files:** `models/analytics.py`, `get_user_security_activity` (line 786)
**How it works:** One query pulls the user's last 50 `analytics_events` rows (any event type, via `user_id`), producing the timeline directly. Recent IPs and User-Agents are derived from the same result set — deduplicated in Python, most-recent-first, capped to 10 IPs / 5 User-Agents in the response to keep the modal readable. Failed logins are queried separately (matched by `metadata::json->>'email'` since they have no `user_id`) and surfaced as a count plus a list of `{timestamp, ip_address}`.
**Access from dashboard:** User detail modal → Security & Activity section.

**Login history — 🟡:** Individual login events **do** appear in the Activity Timeline (each tagged "Logged in" with its timestamp), and the total `login_count` is shown. What does **not** exist is a dedicated, separate "login history" list/table distinct from the general timeline — if you pictured something like a standalone list of the last N logins with their own IPs (as opposed to scrolling the mixed timeline to find them), that specific view isn't there. Data-wise it's fully queryable (`analytics_events WHERE user_id = X AND event_name = 'login'`); it just isn't rendered as its own section.

### 3c. Analytics — ✅ all 8

**Files:** `models/analytics.py`, `get_admin_overview_metrics` (line 867); `admin_routes.py` (line 101, `/dashboard` route context); `templates/admin_dashboard.html` (lines 549–595)

| Metric | Query basis |
|---|---|
| Total users | `COUNT(*) FROM users` |
| New users today | `users.created_at >= CURRENT_DATE` |
| New users this week | `users.created_at >= CURRENT_DATE - INTERVAL '7 days'` |
| Active users (7 days) | `users.last_activity_at >= NOW() - INTERVAL '7 days'` |
| Active users (30 days) | `users.last_activity_at >= NOW() - INTERVAL '30 days'` |
| Shopify connection rate | `COUNT(DISTINCT user) WHERE client_integrations.platform='shopify' AND is_active` ÷ total users — all-time snapshot |
| Signup → Shopify conversion | Of users who signed up in the **last 30 days**, % with Shopify connected now — a real cohort funnel, not the same number restated |
| Shopify → First AI conversation conversion | Of users with Shopify ever connected, % with a `first_ai_conversation` event |

**Access from dashboard:** Dashboard tab (the default/home view) → "User Activity" and "Shopify & AI Engagement" widget rows, below the existing revenue/plan widgets.

### 3d. User Table

| Item | Status |
|---|---|
| Search | 🟡 |
| Sorting | ❌ |
| Filtering — Last login | ❌ |
| Filtering — Shopify status | ❌ |
| Filtering — Suspicion level | ❌ |
| Filtering — Last activity | ❌ |

**Verified directly:** current table headers (`templates/admin_dashboard.html` line 742–748) are `Email, Plan, Status, AI Cost/Mo, Admin, Joined, Actions`. No `Last Login`, `Shopify`, `Suspicion`, or `Last Activity` column exists. The `/users` route (`admin_routes.py` lines 116–142) only queries `models.get_all_users()` and `models.get_user_ai_costs_dict()` — it never calls `get_user_suspicion_scores()`, so suspicion data isn't even fetched for the table, only on-demand per user when the modal opens.

**Search — 🟡:** A `filterUsers(q)` JS function exists and works, client-side, matching against `data-email` on each row. This is **pre-existing** — it was in your original file before any of this work, and I did not extend it to match plan, suspicion level, or any new field.

**Sorting — ❌:** No sort mechanism exists on this table at all — not on the pre-existing columns, and nothing was added for the new ones. No clickable headers, no `sortUsers()` function.

**Filtering by Last login / Shopify / Suspicion / Last activity — ❌:** None of these exist as filter controls, because none of these fields are even present in the table's data or markup yet.

**This entire section (3d) is Component 6 from the original 6-component plan, and it was never built.** Everything in §1–§3c (logging, database, detail modal, dashboard widgets) is real and working; the Users-table-level view is the piece still outstanding.

---

## 4. Bot Detection

| Signal | Status |
|---|---|
| Multiple signups from the same IP | ✅ |
| Disposable email detection | ✅ |
| Random-looking email detection | ✅ |
| No activity after signup | ✅ |
| Never connected Shopify | ✅ |
| Extremely fast repeated registrations | ✅ (additional signal beyond your list, listed separately below) |

**Files:** `models/analytics.py`, `get_user_suspicion_scores` (line 639), `_looks_random` (line 619), `_DISPOSABLE_EMAIL_DOMAINS` (line 601)

**How each signal works, with exact scoring weight:**
- **Extremely fast repeated registrations** (+40): two or more signups from the same IP within 10 minutes of each other.
- **Multiple signups from the same IP in a short time** (+20, mutually exclusive with the above — the fast signal implies this one, so points aren't double-counted): 3+ signups from one IP within a 24-hour span.
- **Disposable email domain** (+25): the email's domain matches a static, embedded list of ~48 known disposable-email providers (Mailinator, Guerrilla Mail, Yopmail, 10minutemail, etc.) — a hardcoded list rather than a paid external API, in line with minimizing costs.
- **Random-looking email username** (+15): a conservative heuristic — flags if the local-part has a digit ratio ≥35%, or is 6+ letters with zero vowels. Deliberately loose; false positives are acceptable since nothing auto-acts on this.
- **No activity after signup** (+15): account is 3+ days old and has exactly one lifetime event (the signup itself, nothing since).
- **Never connected Shopify** (+10): account is 3+ days old (to avoid flagging brand-new signups who just haven't gotten to it yet) and has no active Shopify integration on any of their clients.

**Score bands:** ≥50 → 🔴 High, ≥20 → 🟡 Medium, otherwise 🟢 Low.

**No auto-blocking — ✅, verified:** `grep -rln "suspicion" blueprints/*.py app.py` returns nothing — the suspicion score is referenced **only** inside `admin_routes.py` (for the detail-view JSON response) and `analytics.py` (where it's computed). Nothing in the auth flow, billing flow, or anywhere else checks or acts on this score. It cannot restrict, block, or flag an account for any system behavior — it is read-only, admin-facing data.

**Score + reasons both displayed — ✅, but only in the per-user modal, not the table (see §3d):** the modal's "Bot Detection Signals" section lists every triggered signal in plain language (e.g. "Disposable email domain", "Never connected Shopify") alongside the 🟢/🟡/🔴 badge — you get both the number and the *why* in one view. **Not visible from the Users list without clicking in** — same gap as above.

---

## 5. Existing Functionality — verified not broken

I want to be specific about what "verified" means here rather than just assert it:

- **Every edited file was syntax-checked** (`python3 -m py_compile`) after every change, and the full set together at the end — all pass.
- **`admin_dashboard.html`'s Jinja was parsed** with `jinja2.Environment().parse()` after every template edit — no template syntax errors.
- **The edited inline `<script>` block was extracted and syntax-checked with Node** (with Jinja `{{ }}` expressions stubbed out, since raw Jinja isn't valid JS until rendered) — passes.
- **`track_event()`'s new parameters (`ip_address`, `user_agent`) are optional with `None` defaults** — every pre-existing call site that doesn't pass them (e.g. `password_reset` in `auth.py`) continues to work exactly as before, unmodified.
- **`create_or_link_google_user()`'s new `return_is_new` parameter defaults to `False`**, preserving the old single-value return for any caller that doesn't opt in — only `auth.py`'s one call site was updated to use it.
- **`init_auth()` and `init_chat()`'s call sites in `app.py` were directly checked** (not assumed) — neither function's signature changed, only internal route/closure logic, so both call sites needed no changes and none were made.
- **The Flutterwave webhook's `track_event` call site was deliberately left untouched** — adding IP/UA there would have been *new*, incorrect behavior (recording Flutterwave's server as if it were the customer), not a neutral addition.
- Beyond this admin-dashboard work, one unrelated but real bug was found and fixed while auditing plan-tier consistency: the AI Cost Tracker's Cost-Per-User table was pricing Growth/Scale-plan users as $0/"Free tier" because those tier names weren't in a pricing lookup dict — this was pre-existing, unrelated to components 1–6, and is now fixed (`templates/admin_dashboard.html`, `plan_prices` dict).

---

## 6. Email Verification

**Confirmed: the full flow was intentionally not implemented — correct.** But to be precise about *how much* wasn't implemented, since your audit request's framing doesn't match what actually happened:

You were asked to choose between (a) full flow, (b) schema + backend plumbing only as a placeholder, or (c) skip entirely, and you chose **(c), skip entirely**. As a direct result:
- ❌ No `email_verified` column was added to `users`.
- ❌ No `email_verification` event exists in the logging system.
- ❌ No placeholder field appears anywhere in the admin dashboard (detail modal, Users table, or analytics widgets).

**Nothing is "ready" for a future implementation beyond the fact that the rest of this system (event logging, the detail modal's layout, the dashboard widget grid) is built in a way that a verification feature could plug into later without restructuring anything.** If you want the placeholder-only version now, the smallest version of that would be: one migration adding `users.email_verified BOOLEAN DEFAULT FALSE`, one new event name reserved (`email_verified`) with no code path that fires it yet, and one line in the detail modal reading "Not implemented" or similar. Say the word and I'll build exactly that as its own component.

---

## 7. Final Verification

**Is every implemented feature accessible from the existing admin dashboard without database access or developer-only routes?**
Mostly yes, with one caveat. Everything in §1–§3c (event logging results, the per-user detail modal with all its sections, all 8 dashboard analytics widgets) is reachable purely through clicking around the existing dashboard — Dashboard tab for widgets, Users tab → click a row for everything else. The one exception is the very first admin account ever — granting the first `is_admin = TRUE` is necessarily a chicken-and-egg problem (every `/admin/*` route, including the one that grants admin, requires you to already be an admin), which is why that one step needed raw SQL. Every admin granted after that first one can be done through the System tab's existing "Make Admin" button, no SQL needed.

**Is anything backend-only? If so, list it and explain why.**
Yes, three things:
1. **Users-table sortable/filterable columns for Last Login, Shopify, Suspicion, Last Activity (§3d).** The backend data and queries all exist and work (proven by the same data rendering correctly inside the detail modal) — they're simply not wired into the Users table's route context or template yet. This is Component 6, not started.
2. **Suspicion badges as a table-level/list-level indicator.** Same root cause as #1 — the scoring function works and is called successfully per-user in the modal, but the `/users` route never calls it in bulk for the table.
3. **A dedicated "login history" list separate from the general activity timeline (§3b).** The data is one WHERE clause away (`analytics_events WHERE event_name='login'`), but no separate UI section renders it that way today.

None of these are missing *data* or *logic* — they're missing *table-level UI wiring* for data that's already correctly computed and already displayed at the individual-user level.

**Is anything from the original specification missing?**
Two things, both already covered above but restating plainly:
1. Component 6 in full (§3d / §8 above).
2. Email verification, by your own explicit choice to skip it (§6) — not an oversight, a decision you made.

**What TODOs remain before this feature can be considered production-ready?**
1. Build Component 6 — add `last_login_at`, `login_count`, `last_activity_at`, Shopify status, and suspicion level/badge to the `/users` route's context and the table template, plus client-side sort and filter controls for those columns.
2. Decide on email verification (§6) — leave it out, or add the placeholder-only version.
3. Run the migration (`Run Migrations` in System tab, or it'll run automatically on your next deploy via `app.py`'s startup list) if you haven't already, so `last_login_at`/`login_count`/etc. actually exist in the live database.
4. Spot-check the suspicion scoring's disposable-domain list and random-email heuristic against a sample of your real signups once there's meaningful data — these are reasonable defaults, not tuned against your actual traffic.
5. Decide whether you want a real `user_activity_log`-named table (§2) or are fine with the existing `analytics_events` table serving that role, since your original spec named a table I didn't create under that name.
