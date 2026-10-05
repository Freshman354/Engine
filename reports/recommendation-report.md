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

**Not run:** a live LLM recommendation-generation call. Generation tests used a mocked model response and are not claimed as real provider evaluation.

## Session-scoped chat history

Ran `git grep -n "get_recent_conversations"`. The only runtime call site is `blueprints/chat.py`, which supplies both `client_id` and `session_id` (`limit=15`). The model function requires both arguments and the SQL filters on both. `models/__init__.py` only re-exports the function; the remaining code references are tests and documentation. The characterization test verifies same-client rows from another session are excluded. No caller uses the old unscoped signature.

## Tests

After the final implementation commit, `python -m pytest -q tests` reported **43 passed**. Changed Python modules also passed `py_compile`.

## Known limitation

The Shopify Admin search parser did not apply `description:SUPERDUPER` on this dev store: it returned the full 25-product catalog. Retrieval therefore cannot find a product solely because a search term occurs only in its description unless another supported search field retrieves that product as a candidate. This capability is explicitly reported as a limitation, not a passing evaluation.
