# Product Recommendation Implementation Report

## Summary

Implemented the four requested stages in order and committed each separately:

| Stage | Commit | Change |
|---|---|---|
| 1 | `b9f0bd7` | Expanded Shopify product retrieval to verified fields, limited results to active products, ranked by product relevance and availability, and grouped variants by product. |
| 2 | `ac75e2d` | Added catalog-grounded recommendation generation; an absent, failed, empty, or ungrounded model reply uses the deterministic tool formatter. |
| 3 | `a219d3e` | Added a top-level `products` array to product-tool responses without changing ordinary chat response shapes. |
| 4 | `f1f9355` | Added deterministic need detection and per-client opt-in routing, defaulting off, while preserving existing action/tool precedence and plan gating. |

Each commit was followed by `scripts/eval_recommendations.py` against the supplied dev-store client. Results are recorded below. No Shopify credentials or tokens are written to the report.

## Dev-store query syntax verification

Before changing retrieval, I queried the connected store through Shopify Admin GraphQL. The probe used `products(first: 50, query: $q)` and selected `title`, `description`, `tags`, `productType`, `vendor`, `status`, and `totalInventory`. The dev store returned 25 catalog products.

All rows below are real Admin API responses from that store. Each query returned HTTP 200 with no GraphQL errors.

| Search expression | Actual response |
|---|---|
| `title:"The Complete Snowboard"` | 1 product: `The Complete Snowboard` (`ACTIVE`, inventory `50`). |
| `description:SUPERDUPER` | 25 products, including `The Inventory Not Tracked Snowboard`, `Gift Card`, and `The Out of Stock Snowboard`; the term did not narrow results to the product whose description contains `SUPERDUPER`. |
| `tag:Winter` | 13 products; first three: `The Inventory Not Tracked Snowboard`, `The Out of Stock Snowboard`, `The Compare at Price Snowboard`. |
| `product_type:giftcard` | 1 product: `Gift Card` (`ACTIVE`, type `giftcard`, inventory `179`). |
| `vendor:"Snowboard Vendor"` | 5 products; first three: `Gift Card`, `The Complete Snowboard`, `The Hidden Snowboard`. |
| `status:archived` | 1 product: `The Archived Snowboard` (`ARCHIVED`, inventory `50`). |
| `status:draft` | 1 product: `The Draft Snowboard` (`DRAFT`, inventory `20`). |
| `status:active` | 23 products. |
| `inventory_total:>0` | 22 products; first three: `Gift Card`, `The Compare at Price Snowboard`, `The Complete Snowboard`. |
| `status:active AND inventory_total:>0` | 20 products; first three: `Gift Card`, `The Compare at Price Snowboard`, `The Complete Snowboard`. |
| `status:active AND (title:*snowboard* OR tag:Winter OR product_type:snowboard OR vendor:Chuks)` | 22 products; first results included active snowboard/tag matches such as `The Inventory Not Tracked Snowboard`, `The Out of Stock Snowboard`, and `The Complete Snowboard`. |

**Conclusion from the probes:** title, tag, product type, vendor, status, inventory-total filtering, `AND`/`OR`, and parentheses behaved as expected. `description:` was accepted syntactically but did not filter the result set, so the adapter does not rely on that Shopify search field. Product descriptions are still fetched and used for local ranking among products retrieved through supported search fields.

The live evaluator also observed this real query for `The Complete Snowboard`:

```text
status:active AND (title:*Complete* OR tag:"Complete" OR product_type:"Complete" OR vendor:"Complete" OR title:*Snowboard* OR tag:"Snowboard" OR product_type:"Snowboard" OR vendor:"Snowboard")
```

Shopify returned active candidates whose first five titles were `The Complete Snowboard`, `The Hidden Snowboard`, `The Multi-location Snowboard`, `The Multi-managed Snowboard`, and `The Inventory Not Tracked Snowboard`. Local ranking placed the exact-title product first.

## Behavior and safeguards

- Retrieval queries only the verified Shopify fields and includes `status:active`; archived and draft products are excluded.
- Candidate ranking uses title, tags, product type, description, vendor, and variant options, with availability as a tie-breaker. Matches are returned once per product with variants nested.
- Out-of-stock variants are retained and labeled with their actual availability. The store also has an available-but-untracked variant (`availableForSale=true`, `inventoryQuantity=0`); it remains available rather than being incorrectly rejected by an inventory-total filter.
- Recommendation generation receives only retrieved catalog facts. A generated response is accepted only if it names a retrieved product; otherwise the deterministic formatter is used. If there is no suitable match, that formatter asks for a preferred product type or budget rather than returning a bare “no products found” answer.
- Need detection is a deterministic keyword/pattern check independent of Tier 3. It runs after existing action/tool precedence. An uncertain message is not routed to the recommender and continues through the existing response path.
- New routing is controlled by `bot_settings.product_recommendation_routing_enabled`, settable in Customize Chat Widget and defaulting to `false`. Routing also respects the existing product-recommendation plan gate. The established `search_products` keyword path is unchanged.
- The chat API adds `products` only when a product tool returned a successful result. Non-product response shapes remain unchanged.

## Evaluation after each implementation commit

`PASS` and `LIMITATION` are deliberately distinct. Real-store cases used the dev Shopify API; generator and routing cases were explicitly mock-only and made no model-provider calls.

| Commit | Real dev-store checks | Mock-only checks | Outcome |
|---|---:|---:|---|
| `b9f0bd7` | 6 passed, 0 failed, 1 known limitation | Not present in this stage’s evaluator | All supported retrieval checks passed; `description:` remains a documented limitation. |
| `ac75e2d` | 6 passed, 0 failed, 1 known limitation | 2 passed, 0 failed | Grounded-title acceptance and ungrounded-response fallback passed with mocks. |
| `a219d3e` | 6 passed, 0 failed, 1 known limitation | 3 passed, 0 failed | Additive products contract passed; ordinary replies omit the field. |
| `f1f9355` | 6 passed, 0 failed, 1 known limitation | 8 passed, 0 failed | Need detection, default-off gate, Tier 3 skip/failure, generation fallback, and products contract passed. |

Final real-store cases:

| Scenario | Result | Observed product/result |
|---|---|---|
| Exact product title | PASS | `The Complete Snowboard` ranked first. |
| Tag and product-type retrieval (`winter snowboard`) | PASS | `The Complete Snowboard` returned with `Winter` tag. |
| Vendor retrieval | PASS | Returned products carried the `Snowboard Vendor` metadata. |
| Active products only (`archived snowboard`) | PASS | Active snowboards returned; the archived product was excluded. |
| Out-of-stock variant preservation | PASS | `The Out of Stock Snowboard` returned with `available=false`. |
| Available, untracked variant preservation | PASS | `The Inventory Not Tracked Snowboard` returned with `available=true`, quantity `0`. |
| Description-only `SUPERDUPER` search | LIMITATION | No result from supported search fields; `description:` itself was verified not to filter on this store. |

Final mock-only cases included: generation requiring an exact retrieved title; ungrounded output falling back; a no-match clarification; `products` being additive; need detection with no Tier 3 model; detection when lead and sales are resolved and Tier 3 is skipped; detection when Tier 3 fails and defaults to `question`; and detection while routing is disabled without actually routing.

## Live-provider evaluation for the enabled client

For client `shopify-dev-test-chuks-rjcwoipc-d62b4c9c`, `bot_settings.product_recommendation_routing_enabled` was turned on and read back as enabled. The real provider was Gemini. The environment had `GEMINI_API_KEY`, but `GEMINI_MODEL` was unset and the code default `gemini-2.0-flash` had been retired (the API returned 404). The evaluator now accepts `--model`; runs used `--model gemini-3.8-flash`, as recommended by the API. This is a per-run override, not a persistent model configuration change.

Gemini returned transient HTTP 503 “currently experiencing high demand” failures. Across six complete evaluator invocations (18 generation attempts), 5 returned non-empty text and 13 failed with 503. The final invocation ran the three scenarios with 1 non-empty reply and 2 503 errors. Retrieval was live Shopify; no generation mocks were used.

The exact non-empty replies from all those attempts, grouped by request:

### Under-$500 snowboard request

Retrieved set for both replies: **The Complete Snowboard** ($699.95, available), **The Hidden Snowboard** ($749.95, available), and **The Multi-location Snowboard** ($729.95, available). Each had `Premium`, `Snow`, `Snowboard`, `Sport`, and `Winter` tags.

> We currently do not have any snowboards under $500 in our catalog, as our available options start at $699.95. Would you be open to considering snowboards above your $500 budget?

> We currently do not have any snowboards under $500, as our available models start at $699.95. Would you be open to considering options above that price range?

Neither response names a product outside the retrieved set or ignores the $500 budget. A separate live query for all 12 active snowboard products verified there were no prices under $500 (lowest price: $600.00, `The Collection Snowboard: Hydrogen`). However, the assertion that the catalog “start[s] at $699.95” is false: a snowboard is listed at $600.00, and `The Multi-managed Snowboard` is $629.95. This generated price-floor claim is unsupported by the three retrieved products and contradicted by the full catalog. Neither response named a retrieved product, so the generation acceptance guard rejected both.

The customer-visible fallback in the latest run was:

```
Here are a few catalog options that may fit:
• The Complete Snowboard (color: Ice) — 699.95 — in stock, 10 available
• The Hidden Snowboard (title: Default Title) — 749.95 — in stock, 50 available
• The Multi-location Snowboard (title: Default Title) — 729.95 — in stock, 100 available
```

All three fallback recommendations violate the stated $500 budget. The model itself acknowledged the budget; the fallback did not.

### Winter-sports gift request

Retrieved set: **Gift Card** ($10.00, available; description says store gift card), **The Inventory Not Tracked Snowboard** ($949.95, available; tags `Accessory`, `Sport`, `Winter`), and **The Compare at Price Snowboard** ($785.95, compare-at $885.95, available; product type `snowboard`; tags `Accessory`, `Sport`, `Winter`).

> For a winter sports gift, you can consider [The Compare at Price Snowboard](https://

> For a winter sports gift, you can consider [The Compare at Price Snowboard](https

The named product is in the retrieved set and the data supports its winter-sports category; neither reply states a price or unsupported feature. Both replies have an incomplete Markdown link. They were emitted before the response guard was added and are not customer-ready output.

### Winter snowboard request

Retrieved set: **The Complete Snowboard** ($699.95), **The Hidden Snowboard** ($749.95), and **The Multi-location Snowboard** ($729.95); all are available and carry `Snowboard` and `Winter` tags.

> We have three available winter snowboards: [The Complete Snowboard](https://chuks-r

The count, availability, category, and named product are supported by the retrieved set. No unsupported product, feature, or price is stated. The Markdown link is incomplete and is now rejected by the generator.

After these malformed provider responses were observed, recommendation generation was tightened to reject unfinished Markdown links and use the existing formatter. The evaluator separately audits raw generated replies and customer-visible fallback replies, records provider errors, and reports actual non-empty model replies rather than counting failed calls as generations.

## Session-scoped chat history

Ran `git grep -n "get_recent_conversations"`. The only runtime call site is `blueprints/chat.py`, which supplies both `client_id` and `session_id` (`limit=15`). The model function requires both arguments and the SQL filters on both. `models/__init__.py` only re-exports the function; the remaining code references are tests and documentation. The characterization test verifies same-client rows from another session are excluded. No caller uses the old unscoped signature.

## Tests

After the latest changes, `python -m pytest -q tests` reported **44 passed**. Changed Python modules also passed `py_compile`, and `git diff --check` passed.

## Known limitation

The Shopify Admin search parser did not apply `description:SUPERDUPER` on this dev store: it returned the full 25-product catalog. Retrieval therefore cannot find a product solely because a search term occurs only in its description unless another supported search field retrieves that product as a candidate. This capability is explicitly reported as a limitation, not a passing evaluation.

## Follow-up: numeric price constraints and current Gemini default

On 2026-10-05, the live evaluation exposed two budget defects: price was not considered until after a relevance-only truncation, and the deterministic recommendation formatter presented over-budget items without a caveat. Both are fixed:

- `tools._catalog_search()` now parses price constraints such as `under $500`, `up to $500`, `budget $500`, and `at least $600`. It removes the constraint phrase from relevance scoring, excludes vendor-only false matches when a stronger title/tag/type/description/option match exists, filters to products meeting the price constraint when possible, and orders them by price before relevance tie-breaks. If none fit, it returns the closest priced matching options in distance-to-bound order.
- The result carries the parsed constraint and whether any candidate meets it. The recommendation formatter explicitly states when no product fits, labels closest products as over/under budget, and does not present them as fitting recommendations. With no price-confirmed candidates it asks whether the customer can adjust the limit.
- Gemini now defaults to `gemini-3.8-flash` throughout app config, AI initialization, generation, and usage logging; `GEMINI_MODEL` remains an explicit override. The new [.env.example](../.env.example) documents `GEMINI_MODEL=gemini-3.8-flash`. The API usage log schema default is updated for existing databases when its migration runs.

The evaluator was run against the live dev store and real Gemini, without a `--model` override:

```text
scripts\eval_recommendations.py --client-id shopify-dev-test-chuks-rjcwoipc-d62b4c9c --real-ai
```

The budget retrieval and formatter fallback assertions passed. For “Can you recommend a snowboard under $500?”, the exact retrieved set was:

| Product | Retrieved price | Budget result |
|---|---:|---|
| The Collection Snowboard: Hydrogen | $600.00 | Closest over-budget option |
| The Multi-managed Snowboard | $629.95 | Next closest over-budget option |
| The Complete Snowboard | $699.95 | Also over budget |

The three-call live-provider run used the deterministic formatter because generation failed. Its reply stated that nothing in the catalog fit and identified the alternatives as over budget. I then narrowed that wording to refer to the retrieved matching set rather than imply a catalog-wide search. A subsequent live Shopify retrieval plus formatter-only check (no model calls) for all three requests verified the current output below:

```text
None of the retrieved matching products fit your budget of $500. The closest options are above budget, not within it:
• The Collection Snowboard: Hydrogen (title: Default Title) — 600.00 — in stock, 50 available
• The Multi-managed Snowboard (title: Default Title) — 629.95 — in stock, 100 available
• The Complete Snowboard (color: Ice) — 699.95 — in stock, 10 available
```

The same run attempted all three real-provider scenarios:

| Request | Result |
|---|---|
| “I need a winter snowboard for downhill trips.” | Gemini 429 quota error; formatter listed The Complete Snowboard ($699.95), The Hidden Snowboard ($749.95), and The Multi-location Snowboard ($729.95). |
| “Can you recommend a snowboard under $500?” | Gemini 503 high-demand error; budget-aware fallback shown above. |
| “Suggest a winter sports gift.” | Gemini 429 quota error; formatter listed Gift Card ($10.00), The Inventory Not Tracked Snowboard ($949.95), and The Compare at Price Snowboard ($785.95). |

**Provider reliability:** 1 HTTP 503 among 3 attempts (33.3%), 2 HTTP 429 quota failures (66.7%), and 3/3 total provider failures. No generated replies were returned in this run. The 429 response reported the Gemini free-tier per-day request quota of 20 and a retry delay of about 6h57m. This is a provider/quota reliability risk, not hidden by fallback or reported as a successful generation. Prior live runs with `gemini-3.8-flash` returned text, confirming the model is available, although quality issues were recorded above.

The same invocation reported **6 live Shopify checks passed, 0 failed, 1 description-search limitation**. The latest full test suite reported **49 passed**; changed Python files compiled and `git diff --check` passed.
