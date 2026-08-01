# Admin dashboard update — where each file goes

Every file here is a **complete, final replacement** — drop it straight into
the matching path in your project, overwriting what's there. There are no
duplicates; each filename appears exactly once in this zip.

```
your-project-root/
├── app.py                     ← replace
├── admin_routes.py            ← replace
├── webhooks.py                ← replace
├── bot_protection.py          ← replace
├── blueprints/
│   ├── auth.py                ← replace
│   ├── billing.py             ← replace
│   └── chat.py                ← replace
├── models/
│   ├── __init__.py            ← replace
│   ├── migrations.py          ← replace
│   ├── billing.py             ← replace
│   ├── users.py                ← replace
│   └── analytics.py           ← replace
└── templates/
    └── admin_dashboard.html   ← replace (see note below)
```

**`app.py` — one line added.** It has its own startup migration list (separate
from the admin panel's manual "Run Migrations" button) that auto-runs every
migration on deploy. `migrate_admin_activity_tracking` is now registered
there too — same pattern as every other migration in that list — so it
runs automatically instead of needing a manual click. Nothing else in
`app.py` changed; `init_auth(...)` and `init_chat(...)`'s call sites were
checked and don't need updates — none of this work changed their signatures.

**Note on `admin_dashboard.html`:** I don't have direct confirmation of your
templates folder name — this assumes Flask's default `templates/`. If yours
is named differently, just put the file wherever your other `render_template()`
calls already point.

**Not included** (untouched — nothing to replace): every other blueprint/model
file you have (e.g. `blueprints/account.py`, `models/clients.py`, `models/db.py`,
etc.) — none of those were touched by this work.

## After copying the files in

1. Deploy.
2. In the admin dashboard → **System** tab → click **Run Migrations**. This
   runs `migrate_admin_activity_tracking()`, which adds the new columns/
   indexes. Safe to click even if some migrations already ran before —
   everything is idempotent.
3. That's it — the new Users-table click-through profiles, suspicion badges,
   and dashboard widgets are live.

## What's done vs. what's left

- ✅ Component 1 — migrations
- ✅ Component 2 — event logging (signup/login/logout/failed_login, IP+UA,
  first AI conversation, Shopify connect/disconnect)
- ✅ Component 3 — suspicion/bot scoring
- ✅ Component 4 — per-user detail modal (click any row in Users)
- ✅ Component 5 — dashboard analytics widgets
- ✅ Component 6 — Users table columns (Last Login, Last Activity, Shopify,
  Suspicion), client-side sorting, filters, integrated search

All 6 components are now complete. Email verification remains intentionally
out of scope, as agreed. See `IMPLEMENTATION_AUDIT.md` for the full
components 1-5 audit and `COMPONENT_6_AUDIT.md` for the Component 6
completion audit (query-count verification, no-N+1 confirmation, and
what wasn't broken).

## Plan-tier names fixed (not part of the original component plan)

`admin_dashboard.html` had old plan-tier names (`pro`/`agency`, $99/$299)
left over from before your Starter/Growth/Scale pricing pivot — this
predates this project, not something introduced by components 1-5. Fixed
while addressing it:
- Dashboard's plan stat-cards now show Free/Starter/Growth/Scale (was
  Free/Pro/Agency)
- Users-table plan badges now have `.badge-growth`/`.badge-scale` styles
  (they had no color before — fell back to unstyled)
- The Plan Distribution chart's colors now match the badge colors used
  everywhere else (gold/green/purple by tier) instead of the old
  mismatched purple/orange/red
- The AI Cost Tracker's Cost-Per-User table had a real bug here: Growth/
  Scale users' plan price wasn't in that lookup, so they showed as $0/
  "Free tier" with wrong margin numbers — fixed
- Old `pro`/`agency`/`enterprise` mappings were kept (not deleted) in case
  any existing rows still carry those values
