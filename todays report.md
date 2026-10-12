# Today's Work Report

**Date:** 2026-10-05  
**Repository:** `Freshman354/Engine`  
**Branch:** `shopify-variant-lookup`

## Summary

Implemented Shopify product retrieval/ranking, grounded recommendation replies, an additive product response field, and opt-in recommendation routing. The four code stages were committed individually and the recommendation evaluator was run against the supplied Shopify development client after each commit. I also verified conversation-history session scoping, updated the stale pipeline audit, and wrote the detailed recommendation report at [reports/recommendation-report.md](./reports/recommendation-report.md).

## Changes made

### 1. Retrieval and ranking — `b9f0bd7`

- Updated the Shopify catalog search to retrieve active products using Shopify query fields verified against the dev store: title, tags, product type, and vendor.
- Fetches product tags, product type, vendor, status, and product descriptions alongside product/variant facts.
- Ranks results locally using title, tags, type, description, vendor, and option overlap, with availability as a tie-breaker.
- Groups variants under one product result.
- Retains out-of-stock and available-but-untracked variants with their actual availability and inventory values; it does not equate an inventory quantity of zero with unavailable.
- Added `scripts/eval_recommendations.py` and focused recommendation tests.

### 2. Grounded generation and fallback — `ac75e2d`

- Added a recommendation generation prompt containing only retrieved product facts and the shopper's request.
- A generated response is accepted only when it names a retrieved product. Missing, empty, failed, or ungrounded generation uses the existing deterministic formatter instead.
- After live provider testing returned truncated Markdown links, generation now rejects incomplete Markdown links and falls back to the formatter; a focused regression test covers it.
- The formatter handles no-match recommendations by asking for a product type or budget rather than presenting a bare “no products found” response.
- Original stage checks used mocked responses. A later real-provider evaluation is documented below.

### 3. Additive response products — `a219d3e`

- Added an optional `products` array to the pipeline result and normal `/api/chat` JSON response for successful product tool results.
- Existing non-product responses omit the field, preserving their prior response shape.

### 4. Opt-in need routing — `f1f9355`

- Added deterministic recommendation-need detection independent of Tier 3 classification.
- Detection is performed after existing action/tool precedence; an uncertain or non-need message remains on the existing path.
- Recommendation routing requires both the existing plan eligibility and the client setting `bot_settings.product_recommendation_routing_enabled`.
- The setting defaults to off and can be controlled through Customize Chat Widget. Its saved value is preserved when the customization form omits it.
- Added a formatter-based path for enabled recommendation requests when AI is disabled or unavailable.
- Tests cover no model, Tier 3 skipped because lead/sales are already resolved, Tier 3 failure falling back to `"question"`, disabled setting, plan gate, and AI-disabled API routing.
- Tightened simple-intent matching to whole words after a characterization test exposed that substring matching classified “something” as “hi” and prematurely returned a greeting.

### Documentation — `af91b5d`

- Replaced the earlier proposal-only recommendation report with observed implementation, query, evaluation, limitation, and session-scoping results.
- Corrected stale conversation-history statements in [reports/pipeline-audit.md](./reports/pipeline-audit.md): the API caller and SQL query include both client ID and session ID.

## Shopify dev-store findings

Probes were executed against the connected development store using Shopify Admin GraphQL. Catalog discovery returned 25 products. These are actual queries and observed response summaries; each listed probe returned HTTP 200 with no GraphQL errors:

| Search query | Actual observed result |
|---|---|
| `title:"The Complete Snowboard"` | 1 match: `The Complete Snowboard`, `ACTIVE`, inventory 50. |
| `description:SUPERDUPER` | 25 matches, not narrowed to the product whose description contains `SUPERDUPER`. |
| `tag:Winter` | 13 matches; first results included `The Inventory Not Tracked Snowboard`, `The Out of Stock Snowboard`, and `The Compare at Price Snowboard`. |
| `product_type:giftcard` | 1 match: `Gift Card`, `ACTIVE`, inventory 179. |
| `vendor:"Snowboard Vendor"` | 5 matches; included `Gift Card`, `The Complete Snowboard`, and `The Hidden Snowboard`. |
| `status:archived` | 1 match: `The Archived Snowboard`, status `ARCHIVED`. |
| `status:draft` | 1 match: `The Draft Snowboard`, status `DRAFT`. |
| `status:active` | 23 matches. |
| `inventory_total:>0` | 22 matches. |
| `status:active AND inventory_total:>0` | 20 matches. |
| `status:active AND (title:*snowboard* OR tag:Winter OR product_type:snowboard OR vendor:Chuks)` | 22 matches; active snowboard matches included. |

Example live expression emitted by the evaluator for an exact-name request:

```text
status:active AND (title:*Complete* OR tag:"Complete" OR product_type:"Complete" OR vendor:"Complete" OR title:*Snowboard* OR tag:"Snowboard" OR product_type:"Snowboard" OR vendor:"Snowboard")
```

Its first five raw Shopify results were `The Complete Snowboard`, `The Hidden Snowboard`, `The Multi-location Snowboard`, `The Multi-managed Snowboard`, and `The Inventory Not Tracked Snowboard`; local ranking placed the exact-title match first.

**Discovered limitation:** `description:SUPERDUPER` was syntactically accepted but returned all 25 products. The implementation therefore does not rely on Shopify description search. Descriptions contribute to local ranking only after another supported field retrieves the product. The evaluator records this as a limitation, not a pass.

## Evaluation runs

`scripts/eval_recommendations.py --client-id shopify-dev-test-chuks-rjcwoipc-d62b4c9c` was run after each implementation commit:

| After commit | Real Shopify checks | Mock-only checks | Result |
|---|---:|---:|---|
| `b9f0bd7` | 6 passed, 0 failed, 1 limitation | Not yet included | Exact title, tag/type, vendor, active filtering, out-of-stock preservation, and untracked-inventory preservation passed. |
| `ac75e2d` | 6 passed, 0 failed, 1 limitation | 2 passed, 0 failed | Mocked grounded title accepted; ungrounded generation fell back. |
| `a219d3e` | 6 passed, 0 failed, 1 limitation | 3 passed, 0 failed | Additive products response shape passed; ordinary responses omit it. |
| `f1f9355` | 6 passed, 0 failed, 1 limitation | 8 passed, 0 failed | Tier-independent detection, routing gates, no-match clarification, generation fallback, and response contract passed. |

Mock-only checks are not real Shopify or model-provider evidence. The final evaluator run used real Shopify requests for retrieval and mocks for generation/routing/contract checks.

## Other verification

- Ran `python -m pytest -q tests` after the final changes: **44 passed**.
- Ran `py_compile` on the changed Python modules: completed successfully.
- Ran `git diff --check`: completed successfully.
- Ran `git grep -n "get_recent_conversations"`:
  - The only runtime caller is `blueprints/chat.py`, passing `(client_id, session_id, limit=15)`.
  - `models/conversations.py` requires both IDs and filters its SQL by both.
  - `models/__init__.py` only re-exports the function; the other references are tests and reports.
  - A characterization test confirms another session's rows are excluded.

## Files changed by this work

- `commerce_adapters.py`
- `tools.py`
- `scripts/eval_recommendations.py`
- `tests/test_product_recommendations.py`
- `pipeline/stages/generation.py`
- `pipeline/stages/intent.py`
- `pipeline/context.py`
- `ai_helper.py`
- `blueprints/chat.py`
- `app.py`
- `templates/customize.html`
- `tests/test_chat_pipeline_characterization.py`
- `reports/recommendation-report.md`
- `reports/pipeline-audit.md`

## Worktree note

At final verification, the pre-existing worktree changes were left untouched: modified `scripts/verify_shopify_lookup.py`, and untracked `SHOPIFY_DEV_SEED_REPORT.md`, `app-store-demo-readiness-review.md`, and `scripts/seed_dev_shopify_client.py`. The seed script was inspected but was not part of the recommendation implementation. This report itself is a new uncommitted file.

## Client setting and real-provider evaluation

Enabled `bot_settings.product_recommendation_routing_enabled` for client `shopify-dev-test-chuks-rjcwoipc-d62b4c9c`; read-back and every `--real-ai` evaluator run confirmed it was enabled. The repository had `GEMINI_API_KEY`, but `GEMINI_MODEL` was unset and its code default (`gemini-2.0-flash`) returned 404 because that model is retired. The live evaluator therefore accepted a temporary `--model gemini-3.8-flash` override, recommended by that API error. The replacement model returned intermittent HTTP 503 “currently experiencing high demand” errors.

Across six evaluator invocations (18 live generation attempts), five attempts returned non-empty provider text and 13 failed with the observed 503. The latest invocation completed all seven live Shopify checks (6 PASS, 0 FAIL, 1 documented description-search limitation); one of its three generation calls returned text, and two returned 503. No mocks were used for those live generation attempts.

Every non-empty provider reply captured in those runs is recorded below verbatim. Product evidence is the exact retrieved set from its corresponding request:

### Request: “Can you recommend a snowboard under $500?”

Retrieved products for both replies below:

- **The Complete Snowboard** — $699.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.
- **The Hidden Snowboard** — $749.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.
- **The Multi-location Snowboard** — $729.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.

Reply from an earlier live attempt:

> We currently do not have any snowboards under $500 in our catalog, as our available options start at $699.95. Would you be open to considering snowboards above your $500 budget?

Reply from the latest live evaluation:

> We currently do not have any snowboards under $500, as our available models start at $699.95. Would you be open to considering options above that price range?

**Audit:** Neither reply names a product outside the retrieved set or ignores the stated $500 budget; both acknowledge the budget and ask about exceeding it. The full active Shopify catalog was queried separately: its 12 snowboard products have a minimum variant price of **$600.00** (`The Collection Snowboard: Hydrogen`), so the assertion that there are no snowboards under $500 is supported by that full-catalog check. However, “available options start at $699.95” / “available models start at $699.95” is false: the catalog includes a $600.00 model and `The Multi-managed Snowboard` at $629.95. That price-floor claim is not supported by the three retrieved products either. The evaluator correctly rejected both replies as customer-facing text because neither names a retrieved product.

In the latest run, the user-visible formatter fallback was:

```
Here are a few catalog options that may fit:
• The Complete Snowboard (color: Ice) — 699.95 — in stock, 10 available
• The Hidden Snowboard (title: Default Title) — 749.95 — in stock, 50 available
• The Multi-location Snowboard (title: Default Title) — 729.95 — in stock, 100 available
```

**Budget violation:** all three formatter suggestions exceed $500. The model reply respected the budget, but the fallback shown to the customer did not.

### Request: “Suggest a winter sports gift.”

Retrieved products for both replies:

- **Gift Card** — $10.00; available; description “This is a gift card for the store”.
- **The Inventory Not Tracked Snowboard** — $949.95; available; tags `Accessory`, `Sport`, `Winter`.
- **The Compare at Price Snowboard** — $785.95 (compare-at $885.95); available; product type `snowboard`; tags `Accessory`, `Sport`, `Winter`.

Verbatim replies captured in separate attempts:

> For a winter sports gift, you can consider [The Compare at Price Snowboard](https://

> For a winter sports gift, you can consider [The Compare at Price Snowboard](https

**Audit:** The named product is in the retrieved set and its product type/tags support a winter-sports association. No price or specific feature was claimed. Both replies end in an incomplete Markdown link; after this was observed, generation was changed to reject unfinished Markdown links and use the existing formatter instead. The earlier captured text was produced before that guard and is not considered a valid customer-ready reply.

### Request: “I need a winter snowboard for downhill trips.”

Retrieved products:

- **The Complete Snowboard** — $699.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.
- **The Hidden Snowboard** — $749.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.
- **The Multi-location Snowboard** — $729.95; available; tags `Premium`, `Snow`, `Snowboard`, `Sport`, `Winter`.

Verbatim reply:

> We have three available winter snowboards: [The Complete Snowboard](https://chuks-r

**Audit:** “Three available winter snowboards” is consistent with the three returned products and their availability/tags. It names no product outside the retrieved set and makes no price or feature claim. The link is incomplete, so the new generation guard rejects this form and falls back.

### Final implementation and verification

- `scripts/eval_recommendations.py` now supports a temporary `--model` override, records provider errors instead of leaving failures implicit, audits generated text separately from the customer-visible response, and distinguishes generation attempts from non-empty replies.
- `pipeline/stages/generation.py` rejects an unfinished Markdown link even if the response names a retrieved product; a regression test verifies fallback behavior.
- At that stage: **44 tests passed** (`python -m pytest -q tests`); changed Python files passed `py_compile`; `git diff --check` passed.
- The latest real-store evaluator result: **6 passed, 0 failed, 1 limitation**. The live model result was 1 non-empty reply and 2 provider 503 errors; the model generated the budget response above, while the customer-visible fallback failed the budget check.

## Follow-up: budget constraint repair

After the live run exposed the budget defects, retrieval was updated to parse upper and lower price constraints, filter eligible catalog variants before final relevance ranking, and prioritize matching products by price. When no products meet a maximum budget, only the closest price-confirmed matching products are surfaced, and the formatter explicitly states that they exceed the budget. A regression case includes the cheap `Gift Card` vendor false-match so it cannot displace a snowboard merely because it is under $500.

Gemini defaults were changed from retired `gemini-2.0-flash` to `gemini-3.8-flash` in app config, AI initialization, generator, and usage-log defaults. Added [.env.example](./.env.example) documenting the model and key variables; the usage-log table migration also updates the column default for existing databases when run.

Re-ran all three real dev-store scenarios with the default model and no model CLI override. For **“Can you recommend a snowboard under $500?”**, retrieval returned:

- `The Collection Snowboard: Hydrogen` — **$600.00**
- `The Multi-managed Snowboard` — **$629.95**
- `The Complete Snowboard` — **$699.95**

The three-call live-provider run used the deterministic formatter because generation failed; it explicitly said no item fit the budget and labeled the alternatives over budget. I then narrowed the wording to refer to the retrieved matching set rather than imply a complete catalog-wide search. A subsequent live Shopify retrieval plus formatter-only check (no model calls) verified the current output below:

```
None of the retrieved matching products fit your budget of $500. The closest options are above budget, not within it:
• The Collection Snowboard: Hydrogen (title: Default Title) — 600.00 — in stock, 50 available
• The Multi-managed Snowboard (title: Default Title) — 629.95 — in stock, 100 available
• The Complete Snowboard (color: Ice) — 699.95 — in stock, 10 available
```

The evaluator's budget retrieval and fallback assertions both passed. The other two requests retrieved live data and fell back to the formatter because their provider calls failed.

**Provider risk in this final three-call run:** **1/3 HTTP 503 (33.3%)**, **2/3 HTTP 429 quota errors (66.7%)**, overall failure **3/3 (100%)**, and zero generated replies. The 429 response reported the Gemini free-tier daily per-model request quota (20) was exceeded, with a retry delay of about 6h57m. This is a known provider availability/quota risk. Earlier calls using `gemini-3.8-flash` did return text, confirming availability but also revealing quality issues documented above.

Latest validation: **49 tests passed**; Python compilation and `git diff --check` passed. The live retrieval evaluator reported **6 passed, 0 failed, 1 documented description-search limitation**.
