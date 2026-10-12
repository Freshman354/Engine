# Lumvi App Store Demo Readiness Review

## Overall Ranking

1. **Chat widget:** Medium-high, with prerequisites
2. **Analytics/conversation history:** Medium for single-store demos
3. **Abandoned cart recovery:** Medium-low
4. **Agency dashboard:** Medium-low; single-store only
5. **AI Employee setup:** Low
6. **Product recommendations:** Low

## 1. Chat Widget

**End-to-end:** Partial. The `/api/chat` path loads client FAQs/business knowledge and invokes the AI pipeline. Product lookup reaches live Shopify/WooCommerce adapters through [blueprints/chat.py](blueprints/chat.py#L120) and [tools.py](tools.py#L889).

**Risks:**

- Missing AI credentials can trigger fallback behavior.
- Invalid or missing client IDs silently fall back to hardcoded demo FAQs in [blueprints/chat.py](blueprints/chat.py#L105).
- Product inventory is cached for 60 seconds in [commerce_adapters.py](commerce_adapters.py#L900), so it is not strictly real-time.
- Shopify product lookup requires a separate inventory-enabled OAuth grant.
- The embed script hardcodes `https://lumvi.net` in [static/widget.js](static/widget.js#L18), which can break staging or alternate environments.

**Fastest fix:** Use a verified production AI key and connected Shopify test store, make the widget base URL derive from the script origin, and replace silent demo fallback with an explicit error.

## 2. Agency Dashboard

**End-to-end:** Partial. The authenticated dashboard loads real client rows through [blueprints/auth.py](blueprints/auth.py#L366), but the current product model is one account to one store, not a true agency/multi-store dashboard.

**Risks:**

- The agency blueprint explicitly states the multi-client model was removed in [blueprints/agency.py](blueprints/agency.py#L1).
- The integrations page contains hardcoded fallback stores such as “Bloom Flower Shop” and “Peak Fitness Studio” in [templates/integrations.html](templates/integrations.html#L1450).
- It also displays static “Agency Plan · 7 clients” text in [templates/integrations.html](templates/integrations.html#L990).
- The agency branch is therefore likely to look fake or misleading during a recording.

**Fastest fix:** Demo the authenticated single-store dashboard only. Remove production fallbacks and static agency copy, or restore a real multi-client data model before demonstrating agency functionality.

## 3. Analytics and Conversation History

**End-to-end:** Yes for a single store; no for the agency branch. Conversations are persisted by the chat flow and retrieved into analytics transcripts through [blueprints/agency.py](blueprints/agency.py#L61).

**Risks:**

- The single-store endpoint `/api/admin/analytics` performs real database queries and returns real transcripts.
- The UI calls `/api/analytics/agency` for multi-client mode in [templates/analytics.html](templates/analytics.html#L530), but no corresponding backend route was found.
- Empty or sparse demo accounts will naturally show zeroed charts and empty history.
- There is little end-to-end test coverage for the full analytics workflow.

**Fastest fix:** Remove the dead agency mode from the UI or implement `/api/analytics/agency`. Seed the recording account with real conversations before filming.

## 4. AI Employee Setup

**End-to-end:** Partial. Shopify/WooCommerce connection and final branding/FAQ saves are real. The import stage is not.

**Risks:**

- The onboarding code explicitly says import is simulated in [templates/onboarding.html](templates/onboarding.html#L857).
- It hardcodes counts such as `142 imported` and `9 imported`.
- The preview uses “Sample product from your store” and `$—` in [templates/onboarding.html](templates/onboarding.html#L925).
- FAQ entries are placeholder text claiming to come from store policies.
- No `/api/import/start` or `/api/import/status` implementation was found.

**Fastest fix:** For a demo, remove the fake counts and show the real connection result plus a live product lookup. For production, implement import/status endpoints and populate FAQs from actual store data.

## 5. Abandoned Cart Recovery

**End-to-end:** Partial. The Shopify webhook-to-database-to-cron flow exists:

- Checkout webhooks persist carts in [webhooks.py](webhooks.py#L1572).
- Completed orders mark carts recovered in [webhooks.py](webhooks.py#L1615).
- Cron claims carts and sends recovery emails in [blueprints/cron.py](blueprints/cron.py#L500).

**Risks:**

- It is Shopify-only; no equivalent WooCommerce abandoned-cart webhook path was found.
- It depends on configured mail delivery, cron execution, Brevo inbound parsing, DNS/MX records, and reply-domain configuration.
- A real recording cannot reliably prove the flow without a real abandoned checkout and delivered email.
- Tests mostly mock persistence and external services.

**Fastest fix:** Add a startup/health check for mail, cron, Brevo, and DNS. Test with one real Shopify checkout and verify delivery before recording. Do not demo the preview email as proof of live recovery.

## 6. Product Recommendation Logic

**End-to-end:** No as a true recommendation workflow; partial as contextual catalog search.

**Risks:**

- `recommend_products()` exists in [tools.py](tools.py#L993), but it shares the same live search implementation as `search_products`.
- There is no personalization, purchase-history, or ranking engine.
- The normal intent path explicitly prepares `search_products` arguments in [pipeline/stages/intent.py](pipeline/stages/intent.py#L230), but no equivalent recommendation intent path was found.
- “Recommend something for dry skin” may therefore become generic search, RAG, or a normal AI response instead of a recommendation tool call.
- Live inventory results are still cached for 60 seconds.

**Fastest fix:** Wire recommendation intent detection and argument extraction into the agent pipeline. For the immediate demo, describe this honestly as “contextual product matching,” not personalized recommendations.

## Recommended Recording Path

Use one real Shopify store, verify AI credentials and product access, ask the widget product and FAQ questions, then show single-store analytics populated by those conversations. Avoid presenting the onboarding import counts, multi-client agency view, or proactive recommendations as fully live until the fixes above are made.
