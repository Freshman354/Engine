# Shopify Recommendation Work Report

## 1. Summary

No recommendation implementation was made in this session; the work here was a proposal only.
The existing `recommend_products()` function remains a wrapper around the same catalog search, and recommendation phrasing is not newly routed to it.
No Shopify request, recommendation evaluation, or pytest run was performed in this session.
The report records current source behavior and the checks that remain before implementation/merge.

## 2. Commits

There are no recommendation implementation commits on the branch. The relevant earlier Prompt A commits, in chronological order, are:

| Commit | Message | Prompt A contents |
| --- | --- | --- |
| `43cb52a` | `Add chat routing characterization tests` | Added `tests/test_chat_pipeline_characterization.py` to characterize simple/action/tool precedence, product keyword dispatch, normal `/api/chat` response JSON, and session-history isolation. |
| `d29ba67` | `Scope chat history to conversation session` | Updated `models.get_recent_conversations()` and its `/api/chat` caller to filter AI history by both client and session. The test asserts only the requested session is returned. |

Prompt A was done and committed in those two changes. The current session made no code changes before creating this report. The pre-existing untracked `app-store-demo-readiness-review.md` was not changed.

## 3. Routing Change (Step 1)

**Actual state:** No routing change was implemented. Recommendation/gift/budget/comparison routing described below is the earlier proposal, not current behavior.

[Read in code] Current intent precedence remains simple intent, then action, then tool keyword, followed by retrieval when none returns early ([intent.py](../pipeline/stages/intent.py#L625), [ai_helper.py](../ai_helper.py#L1210), [ai_helper.py](../ai_helper.py#L1250)). Current keyword matches for `search_products` are the entries in `TOOL_KEYWORDS` ([constants.py](../constants.py#L147)). `recommend_products` is present in `tools.py`, but is not included in that keyword map ([tools.py](../tools.py#L1072), [constants.py](../constants.py#L112)). The existing Gemini Tier 3 classifier asks for a general intent enum and lead/sales flags; it does not currently classify a recommendation tool name ([intent.py](../pipeline/stages/intent.py#L537)).

The proposal was to keep the current intent order and existing product keyword matches as the fast path, route recommendation/gift/budget/comparison wording to `recommend_products`, and extend the existing Tier 3 classifier for unmatched needs when recommendation access is plan-allowed and an inventory-capable Shopify adapter exists. No such classifier extension or inventory-integration gate was implemented or tested. The current plan gate is on the `search_products` branch, not a newly implemented recommendation branch ([ai_helper.py](../ai_helper.py#L1270)).

| Example message | Current route | After this session |
| --- | --- | --- |
| `do you sell blue widget` | `search_products` keyword tool ([constants.py](../constants.py#L147)) | Same; no change was implemented. |
| `something for dry skin` | No listed product-keyword match; proceeds through later intent/retrieval handling ([constants.py](../constants.py#L147), [intent.py](../pipeline/stages/intent.py#L585)) | Same; proposed recommendation route not implemented. |
| `gift under £30` | No listed product-keyword match; proceeds through later intent/retrieval handling ([constants.py](../constants.py#L147), [intent.py](../pipeline/stages/intent.py#L585)) | Same; proposed recommendation route not implemented. |
| `compare a blue and red moisturizer` | No listed product-keyword match in this wording; proceeds through later intent/retrieval handling ([constants.py](../constants.py#L147), [intent.py](../pipeline/stages/intent.py#L585)) | Same; proposed comparison routing not implemented. |
| `what goes well with coffee` | `search_products` keyword tool because `goes well with` is listed ([constants.py](../constants.py#L147)) | Same; no change was implemented. |

The examples are source-based predictions of current routing; they were not executed as runtime tests in this session. Potentially captured intents were not tested because no routing change was made. The proposal intended to preserve simple/action/tool precedence; whether added recommendation signals would collide with existing action/tool phrases remains unverified.

## 4. Retrieval

[Read in code] Current `recommend_products(client_id, customer_need, limit)` calls `_catalog_search()` with the raw `customer_need`; it does not expand the need into synonyms, use case, product type, or multiple terms ([tools.py](../tools.py#L1072), [tools.py](../tools.py#L955)).

[Read in code] Current Shopify GraphQL query is `products(first: 5, query: $q)` and requests the query variable as `title:*{query}*`. It selects product title, handle, description, featured image, then up to ten variants with title, selected options, availability, inventory quantity, price, compare-at price and image ([commerce_adapters.py](../commerce_adapters.py#L238)). It does not request tags, product type, vendor, or product status in this query. The query string template is the actual source query; no live request was made.

A source-derived example of the constructed Shopify search expression, **not an observed request**, is `title:*dry skin*` for query `dry skin`; the code interpolates the sanitized query into `title:*{query}*` ([commerce_adapters.py](../commerce_adapters.py#L263)). No real query example can be supplied because no dev-store `--client-id` was provided and no Shopify request ran.

**Dev-store verification:** none performed. No Shopify query syntax or support for title, description, tags, type, or vendor search was verified against a dev store. Expanding search syntax and its field coverage were proposed only. Claims about which Shopify query fields work beyond the current title expression would be assumptions and are not made here.

## 5. Ranking and Filters

[Read in code] Current Shopify results follow Shopify's returned product/variant edge order; the adapter flattens each variant into an `InventoryMatch`. The shared recommendation/search function does not rank on need relevance, stock, or budget and does not filter inactive products or out-of-stock variants ([commerce_adapters.py](../commerce_adapters.py#L288), [tools.py](../tools.py#L988)).

[Read in code] Current `recommend_products()` returns the common catalog result, with the requested limit capped at five; it does not produce one product object with nested variants ([tools.py](../tools.py#L1072), [tools.py](../tools.py#L966)). `search_products()` exposes flattened variant entries. No active-status condition or budget parser exists in the inspected recommendation function.

No relevance scoring, availability-first tie-break, budget-fit ranking, active/in-stock filter, or nested variant result was implemented or run. One entry per product with variants nested was part of the proposal only.

## 6. Answer Generation

[Read in code] Current `recommend_products()` does not make a generation call; it returns a structured catalog result. The fixed keyword tool route formats tool data to text and returns it as the current response rather than passing that result back to another model call ([ai_helper.py](../ai_helper.py#L1300), [intent.py](../pipeline/stages/intent.py#L275)).

No recommendation prompt was added. Thus there is no implemented prompt to quote, no verified grounding behavior for generated recommendations, no implemented “no fit, ask one clarifying question” behavior, and no recommendation-specific fallback generation. The earlier proposal was one grounded generation call after retrieval, with the existing formatter as fallback on model failure; it was not implemented.

No recommendation-turn model calls were observed or counted. The existing shared `ctx.call1_used` logic and generation path remain unchanged ([ai_helper.py](../ai_helper.py#L1230), [ai_helper.py](../ai_helper.py#L1540), [ai_helper.py](../ai_helper.py#L1578)). Any claim about recommendation-turn call count would be speculative until the route is implemented and exercised.

## 7. Response Contract

No response-contract change was made. The normal AI-success branch currently returns this shape, with values dependent on the request:

```json
{
  "success": true,
  "response": "<reply string>",
  "confidence": 0.82,
  "method": "rag",
  "session_id": "<request session id>"
}
```

[Read in code] This shape is constructed by the normal AI branch in `blueprints/chat.chat()` ([chat.py](../blueprints/chat.py#L600)). Other branches have different additive fields, for example `trigger_lead_collection`, `contact_info`, `limit_reached`, or `show_contact_button` ([chat.py](../blueprints/chat.py#L373), [chat.py](../blueprints/chat.py#L480), [chat.py](../blueprints/chat.py#L650)).

“Before” and “after” are identical because no recommendation response change was implemented. `response` remains a string. There is no new `products` response array; additive `products` support was proposed but not implemented or tested.

## 8. Invariants Check

| Invariant | Status this session | Evidence |
| --- | --- | --- |
| Intent precedence | Unchanged; no routing code changed. | Existing order is simple → action → tool → retrieval ([intent.py](../pipeline/stages/intent.py#L625), [ai_helper.py](../ai_helper.py#L1250)). |
| Plan gating | Unchanged; no recommendation gate added. | The existing helper call receives `product_recommendations_enabled`; the production fixed-tool branch shown gates `search_products` ([blueprints/chat.py](../blueprints/chat.py#L418), [ai_helper.py](../ai_helper.py#L1283)). |
| Server-side `client_id` scoping | Unchanged; no new dispatch or credential path added. | Existing fixed dispatch injects server-side client ID ([intent.py](../pipeline/stages/intent.py#L123)); commerce adapter resolution takes that client ID ([commerce_adapters.py](../commerce_adapters.py#L750)). |
| Inventory cache key includes `client_id` | Unchanged. | Current format is `{client_id}:{query}:{option_key}` ([commerce_adapters.py](../commerce_adapters.py#L948)). |
| Method strings and analytics meaning | Unchanged. | No logging, method, matched, or analytics code was modified in this session; existing normal route logs `method` and `matched` ([blueprints/chat.py](../blueprints/chat.py#L545)). |
| Two-call model budget | Unchanged; recommendation logic was not implemented. | Existing `call1_used` checks remain as in the helper ([ai_helper.py](../ai_helper.py#L1230), [ai_helper.py](../ai_helper.py#L1540), [ai_helper.py](../ai_helper.py#L1578)). |

These are source/worktree checks, not runtime tests of the invariants during recommendation behavior.

## 9. Test Results

**Recommendation work in this session:** no tests were run. There is no `scripts/eval_recommendations.py` in the current worktree, so there is no per-query eval output and no PASS/FAIL table to report. No real store or mock-only recommendation evaluation ran.

**Previously completed Prompt A, from the earlier task:** `venv\Scripts\python.exe -m pytest -q tests` reported **26 passed in 0.87s** after the characterization and history-isolation changes. This was a prior run, not a recommendation test and not run in this report-writing session. The Prompt A characterization test file is [tests/test_chat_pipeline_characterization.py](../tests/test_chat_pipeline_characterization.py).

No recommendation behavior, product-name grounding, out-of-stock exclusion, budget filtering, or Shopify query syntax is covered by that prior result.

## 10. Not Verified

- No dev-store client ID was supplied; no live Shopify request was made.
- Shopify search syntax for description, tags, product type, vendor, status, and multi-term expressions was not verified.
- No recommendation routing, search-term expansion, product-level ranking, active/in-stock filtering, budget matching, nested variants, recommendation prompt, or optional `products` response field was implemented or tested.
- No `scripts/eval_recommendations.py` exists in the inspected branch; the proposed 20+ cases did not run.
- No actual model/provider availability or recommendation cost/latency was measured.
- No tests were run during this report-writing session; the 26-pass result above is explicitly from the earlier Prompt A work.

## 11. Known Issues and Risks

- [Read in code] Shopify's existing inventory query is title-only and uses one catalog request per uncached query; it requests up to five products and ten variants per product ([commerce_adapters.py](../commerce_adapters.py#L238)). Whether broader query fields are supported is unverified.
- [Inferred] Multi-term expansion can multiply Shopify calls if implemented as one request per term; batching or combining terms may affect recall and Shopify GraphQL cost. No change or measurement was made.
- [Read in code] Inventory cache is in-process, 60-second TTL, and keyed by tenant/query/options ([commerce_adapters.py](../commerce_adapters.py#L938)). [Inferred] Recommendation term expansion, budget, exclusions, or ranking inputs need to be reflected in any cache design if they alter returned result semantics; none was implemented.
- [Inferred] A grounded generation step would add latency and token cost; exact amount depends on provider, prompt, catalog result size, and whether the existing first-call budget slot was already consumed. No timing or cost experiment ran.
- [Inferred] The requirement to filter active products, in-stock products, and budget fit depends on query fields and pricing/variant semantics being fetched correctly; none was verified against Shopify.

## 12. Next Steps Before Merge

1. Obtain a dev-store `--client-id` and verify the actual Shopify query syntax/fields against its catalog.
2. Approve/adjust the previously supplied implementation proposal; none of its recommendation code changes has been applied.
3. Implement routing/retrieval/ranking/generation/optional response products in separately reviewable commits while preserving the invariants in section 8.
4. Add and run `scripts/eval_recommendations.py` with at least 20 cases; report each query and automated check result, identifying mock vs. real-store execution.
5. Run the full pytest-managed suite and targeted tests; measure model calls, latency and Shopify GraphQL errors/rate limiting before merge.
