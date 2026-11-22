"""Evaluate live product search/recommendation retrieval against a Shopify client."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", required=True)
    parser.add_argument(
        "--real-ai",
        action="store_true",
        help="call the configured live provider for recommendation replies; no mock generation",
    )
    parser.add_argument(
        "--model",
        help="temporary Gemini model override for --real-ai; defaults to provider configuration",
    )
    return parser


def _shopify_products(payload: dict[str, Any]) -> list[dict[str, Any]]:
    edges = (((payload.get("data") or {}).get("products") or {}).get("edges") or [])
    return [edge.get("node", {}) for edge in edges]


def _run_case(
    label: str,
    client_id: str,
    query: str,
    argument: str,
    search: Callable[..., dict],
    expected: Callable[[dict], bool],
) -> dict[str, Any]:
    import requests
    from unittest.mock import patch
    import commerce_adapters

    commerce_adapters._INVENTORY_CACHE.clear()
    raw: list[dict[str, Any]] = []
    original_post = requests.post

    def capture_post(url: str, *args: Any, **kwargs: Any):
        response = original_post(url, *args, **kwargs)
        if "/graphql.json" in str(url):
            try:
                payload = response.json()
            except ValueError:
                payload = {"body": "<non-JSON response>"}
            raw.append({
                "http_status": response.status_code,
                "search_expression": ((kwargs.get("json") or {}).get("variables") or {}).get("q"),
                "graphql_errors": payload.get("errors", []),
                "products": [
                    {
                        "title": product.get("title"),
                        "status": product.get("status"),
                        "tags": product.get("tags"),
                        "product_type": product.get("productType"),
                        "vendor": product.get("vendor"),
                        "inventory": product.get("totalInventory"),
                    }
                    for product in _shopify_products(payload)[:5]
                ],
            })
        return response

    with patch.object(requests, "post", side_effect=capture_post):
        result = search(client_id=client_id, **{argument: query})
    passed = bool(expected(result))
    result_summary = {
        "success": result.get("success"),
        "count": result.get("count"),
        "response_state": result.get("response_state"),
        "products": [
            {
                "title": product.get("title"),
                "available": product.get("available"),
                "quantity": product.get("quantity"),
                "price": product.get("price"),
                "tags": product.get("tags"),
                "product_type": product.get("product_type"),
                "vendor": product.get("vendor"),
                "variants": product.get("variants"),
            }
            for product in result.get("products", [])
        ],
    }
    return {
        "scenario": label,
        "input": query,
        "status": "PASS" if passed else "FAIL",
        "result": result_summary,
        "raw_shopify_response": raw[-1] if raw else None,
    }


def _run_mock_generation_cases() -> list[dict[str, Any]]:
    from types import SimpleNamespace
    from unittest.mock import patch

    from pipeline.stages.generation import generate_product_recommendation_response
    from pipeline.stages.intent import (
        _build_tool_response_text,
        detect_intent,
        detect_product_recommendation_intent,
        should_route_product_recommendation,
    )
    from pipeline.context import PipelineResult

    products = [{
        "title": "The Complete Snowboard",
        "description": "A premium snowboard.",
        "available": True,
        "price": "699.95",
        "product_url": "https://example.invalid/products/the-complete-snowboard",
    }]
    generated = "The Complete Snowboard is listed at 699.95."
    with patch(
        "pipeline.stages.generation._gemini_generate",
        return_value=SimpleNamespace(text=generated),
    ):
        grounded = generate_product_recommendation_response(
            "Find a winter snowboard", products, model=object(),
        )
    fallback = _build_tool_response_text(
        "recommend_products",
        {"products": products},
    )
    no_match_fallback = _build_tool_response_text(
        "recommend_products",
        {"products": []},
    )

    with patch(
        "pipeline.stages.generation._gemini_generate",
        return_value=SimpleNamespace(text="Try the Alpine Pro 9000."),
    ):
        ungrounded = generate_product_recommendation_response(
            "Find a winter snowboard", products, model=object(),
        )

    contract_result = PipelineResult(
        response=grounded or fallback,
        method="product_recommendation",
        confidence=0.9,
        products=products,
    ).to_dict()
    unrelated_result = PipelineResult(
        response="A standard answer.",
        method="rag",
        confidence=0.8,
    ).to_dict()
    no_model_message = "I need something for dry skin"
    no_model_intent = detect_intent(
        no_model_message, "ecommerce", [], model=None, skip_gemini=True,
    )
    resolved_message = "I'm looking for a winter moisturizer; what is the price?"
    with patch("pipeline.stages.intent.classify_intent_gemini") as classify:
        resolved_intent = detect_intent(
            resolved_message, "ecommerce", [], model=object(), skip_gemini=False,
        )
    failed_message = "I'm looking for a winter moisturizer"
    with patch(
        "pipeline.stages.intent._gemini_generate",
        side_effect=RuntimeError("mock provider failure"),
    ):
        failed_intent = detect_intent(
            failed_message, "ecommerce", [], model=object(), skip_gemini=False,
        )

    return [
        {
            "scenario": "grounded generation accepts retrieved title",
            "status": "PASS" if grounded == generated else "FAIL",
            "execution": "mock-only",
        },
        {
            "scenario": "ungrounded generation falls back",
            "status": (
                "PASS"
                if ungrounded is None and "The Complete Snowboard" in fallback
                else "FAIL"
            ),
            "execution": "mock-only",
            "fallback": fallback,
        },
        {
            "scenario": "no catalog match asks a clarifying question",
            "status": (
                "PASS"
                if "preferred product type or budget" in no_match_fallback
                and "No products found" not in no_match_fallback
                else "FAIL"
            ),
            "execution": "mock-only",
            "fallback": no_match_fallback,
        },
        {
            "scenario": "products array is additive and omitted for ordinary answers",
            "status": (
                "PASS"
                if contract_result.get("products") == products
                and "products" not in unrelated_result
                else "FAIL"
            ),
            "execution": "mock-only",
        },
        {
            "scenario": "need detected without Tier 3 model",
            "status": (
                "PASS"
                if no_model_intent["intent"] == "question"
                and should_route_product_recommendation(no_model_message, True, True)
                else "FAIL"
            ),
            "execution": "mock-only",
        },
        {
            "scenario": "need detected when lead and sales skip Tier 3",
            "status": (
                "PASS"
                if resolved_intent["is_lead"] and resolved_intent["is_sales"]
                and not classify.called
                and detect_product_recommendation_intent(resolved_message)
                else "FAIL"
            ),
            "execution": "mock-only",
        },
        {
            "scenario": "need detected after Tier 3 failure defaults to question",
            "status": (
                "PASS"
                if failed_intent["intent"] == "question"
                and detect_product_recommendation_intent(failed_message)
                else "FAIL"
            ),
            "execution": "mock-only",
        },
        {
            "scenario": "disabled opt-in detects but does not route",
            "status": (
                "PASS"
                if detect_product_recommendation_intent(no_model_message)
                and not should_route_product_recommendation(
                    no_model_message, False, True,
                )
                else "FAIL"
            ),
            "execution": "mock-only",
        },
    ]


def _price_values(products: list[dict[str, Any]]) -> set[str]:
    values = set()
    for product in products:
        for key in ("price", "compare_at_price"):
            value = product.get(key)
            if value is not None:
                values.add(f"{float(value):.2f}")
        for variant in product.get("variants") or []:
            for key in ("price", "compare_at_price"):
                value = variant.get(key)
                if value is not None:
                    values.add(f"{float(value):.2f}")
    return values


def _audit_real_reply(
    request: str,
    reply: str,
    products: list[dict[str, Any]],
    catalog_titles: list[str],
) -> list[str]:
    flags = []
    reply_lower = reply.casefold()
    if re.search(r"\[[^\]]+\]\([^)]*$", reply):
        flags.append("reply ends with an incomplete Markdown link")
    budget_match = re.search(
        r"\b(?:under|below|less than|up to|no more than|maximum(?: of)?)\s*"
        r"[$£€]?\s*(\d[\d,]*(?:\.\d{1,2})?)",
        request,
        re.IGNORECASE,
    )
    stated_budget = (
        float(budget_match.group(1).replace(",", ""))
        if budget_match else None
    )
    retrieved_titles = {
        str(product.get("title") or "").casefold()
        for product in products
    }
    mentioned_catalog_titles = [
        title for title in catalog_titles
        if title and title.casefold() in reply_lower
    ]
    outside_retrieved = [
        title for title in mentioned_catalog_titles
        if title.casefold() not in retrieved_titles
    ]
    if outside_retrieved:
        flags.append(
            "names catalog product(s) outside retrieved set: "
            + ", ".join(outside_retrieved)
        )

    allowed_prices = _price_values(products)
    for match in re.finditer(
        r"([$£€])\s*(\d[\d,]*(?:\.\d{1,2})?)|"
        r"(\d[\d,]*\.\d{1,2})",
        reply,
    ):
        raw_price = match.group(2) or match.group(3)
        numeric = raw_price.replace(",", "")
        parsed_price = float(numeric)
        if stated_budget is not None and parsed_price == stated_budget:
            continue
        if f"{parsed_price:.2f}" not in allowed_prices:
            flags.append(f"mentions price/amount {raw_price} not present in retrieved data")

    if budget_match:
        budget = stated_budget
        assert budget is not None
        recommended = [
            product for product in products
            if product.get("title")
            and str(product["title"]).casefold() in reply_lower
        ]
        over_budget = []
        for product in recommended:
            prices = []
            for key in ("price", "compare_at_price"):
                if product.get(key) is not None:
                    prices.append(float(product[key]))
            for variant in product.get("variants") or []:
                if variant.get("price") is not None:
                    prices.append(float(variant["price"]))
            if prices and min(prices) > budget:
                over_budget.append(str(product["title"]))
        if over_budget:
            flags.append(
                f"recommends product(s) over stated budget {budget:g}: "
                + ", ".join(over_budget)
            )

    named_products = [
        product for product in products
        if product.get("title")
        and str(product["title"]).casefold() in reply_lower
    ]
    positive_availability = ("in stock", "available")
    negative_availability = ("sold out", "out of stock")
    if any(term in reply_lower for term in positive_availability):
        unavailable_named = [
            str(product["title"]) for product in named_products
            if not product.get("available")
        ]
        if unavailable_named:
            flags.append(
                "claims availability for unavailable product(s): "
                + ", ".join(unavailable_named)
            )
        elif not named_products and products and not any(
            bool(product.get("available")) for product in products
        ):
            flags.append("claims availability but all retrieved products are unavailable")
    if any(term in reply_lower for term in negative_availability):
        available_named = [
            str(product["title"]) for product in named_products
            if product.get("available")
        ]
        if available_named:
            flags.append(
                "claims out-of-stock status for available product(s): "
                + ", ".join(available_named)
            )

    return flags


def _run_real_generation_cases(
    client_id: str,
    adapter: Any,
    model_override: str | None = None,
) -> list[dict[str, Any]]:
    from google import genai

    from pipeline.stages import generation
    from pipeline.stages.intent import _build_tool_response_text
    from tools import recommend_products
    from utils import get_ai_provider

    provider = get_ai_provider()
    if provider == "gemini":
        api_key = os.environ.get("GEMINI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "AI_PROVIDER resolves to gemini but GEMINI_API_KEY is missing."
            )
        model_name = (
            model_override
            or os.environ.get("GEMINI_MODEL", "gemini-3.8-flash")
        ).strip()
        os.environ["GEMINI_MODEL"] = model_name
        genai_client = genai.Client(api_key=api_key)
        model = genai_client.models
    elif provider == "openrouter":
        api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError(
                "AI_PROVIDER resolves to openrouter but OPENROUTER_API_KEY is missing."
            )
        from utils import OPENROUTER_MODEL

        model_name = OPENROUTER_MODEL
        model = object()
    else:
        raise RuntimeError(f"Unsupported live provider: {provider}")

    import requests

    access_token = adapter._get_access_token()
    response = requests.post(
        f"https://{adapter.shop_domain}/admin/api/{adapter.API_VERSION}/graphql.json",
        json={"query": "query RecommendationAuditCatalog { products(first: 250) { edges { node { title } } } }"},
        headers={"X-Shopify-Access-Token": access_token},
        timeout=15,
    )
    if response.status_code != 200:
        raise RuntimeError(
            f"Catalog title audit request returned HTTP {response.status_code}"
        )
    payload = response.json()
    if payload.get("errors"):
        raise RuntimeError("Catalog title audit request returned GraphQL errors")
    catalog_titles = [
        str(product.get("title") or "")
        for product in _shopify_products(payload)
    ]

    cases = [
        "I need a winter snowboard for downhill trips.",
        "Can you recommend a snowboard under $500?",
        "Suggest a winter sports gift.",
    ]
    original_generate = generation._gemini_generate
    results = []
    try:
        for shopper_request in cases:
            raw_model_replies: list[str] = []
            raw_model_providers: list[str] = []
            raw_model_names: list[str] = []
            provider_errors: list[str] = []

            def capture_real_call(*args: Any, **kwargs: Any):
                try:
                    result = original_generate(*args, **kwargs)
                except Exception as error:
                    provider_errors.append(
                        f"{type(error).__name__}: {error}"
                    )
                    raise
                raw_model_replies.append(
                    str(getattr(result, "text", None) or "")
                )
                raw_model_providers.append(
                    str(getattr(result, "provider", None) or provider)
                )
                raw_model_names.append(
                    str(getattr(result, "model", None) or model_name)
                )
                return result

            generation._gemini_generate = capture_real_call
            retrieved = recommend_products(
                client_id=client_id,
                customer_need=shopper_request,
            )
            products = retrieved.get("products", []) if retrieved.get("success") else []
            raw_reply = None
            accepted_reply = None
            error = retrieved.get("error")
            if products:
                accepted_reply = generation.generate_product_recommendation_response(
                    shopper_request,
                    products,
                    model,
                )
                raw_reply = raw_model_replies[-1] if raw_model_replies else None
                visible_reply = accepted_reply or _build_tool_response_text(
                    "recommend_products",
                    retrieved,
                )
            else:
                visible_reply = _build_tool_response_text(
                    "recommend_products",
                    retrieved if retrieved.get("success") else {"products": []},
                )
            formatter_reply = _build_tool_response_text(
                "recommend_products",
                retrieved if retrieved.get("success") else {"products": []},
            )

            generated_audit_flags = (
                _audit_real_reply(
                    shopper_request,
                    raw_reply,
                    products,
                    catalog_titles,
                )
                if raw_reply else []
            )
            visible_audit_flags = _audit_real_reply(
                shopper_request,
                visible_reply,
                products,
                catalog_titles,
            )
            budget_retrieval_check = None
            budget_fallback_check = None
            if shopper_request == "Can you recommend a snowboard under $500?":
                retrieved_prices = {
                    str(product.get("title")): {
                        round(float(price), 2)
                        for price in [
                            product.get("price"),
                            *[
                                variant.get("price")
                                for variant in product.get("variants") or []
                            ],
                        ]
                        if price is not None
                    }
                    for product in products
                }
                budget_retrieval_check = {
                    "status": (
                        "PASS"
                        if retrieved_prices.get("The Collection Snowboard: Hydrogen")
                        and 600.0 in retrieved_prices["The Collection Snowboard: Hydrogen"]
                        and retrieved_prices.get("The Multi-managed Snowboard")
                        and 629.95 in retrieved_prices["The Multi-managed Snowboard"]
                        else "FAIL"
                    ),
                    "expected_candidates": {
                        "The Collection Snowboard: Hydrogen": 600.0,
                        "The Multi-managed Snowboard": 629.95,
                    },
                    "retrieved_candidates": retrieved_prices,
                }
                budget_fallback_check = {
                    "status": (
                        "PASS"
                        if not retrieved.get("price_constraint_satisfied", True)
                        and "None of the retrieved matching products fit your budget of $500"
                        in formatter_reply
                        and "above budget, not within it" in formatter_reply
                        else "FAIL"
                    ),
                    "formatter_reply": formatter_reply,
                }
            results.append({
                "execution": "real-provider",
                "provider": raw_model_providers[-1] if raw_model_providers else None,
                "model": raw_model_names[-1] if raw_model_names else None,
                "configured_provider": provider,
                "shopper_request": shopper_request,
                "raw_model_reply_verbatim": raw_reply,
                "accepted_generated_reply": accepted_reply,
                "customer_visible_reply": visible_reply,
                "formatter_fallback_reply": formatter_reply,
                "grounded_products": products,
                "provider_error": provider_errors[-1] if provider_errors else None,
                "provider_error_status": (
                    re.search(r"\b(429|503)\b", provider_errors[-1]).group(1)
                    if provider_errors
                    and re.search(r"\b(429|503)\b", provider_errors[-1])
                    else None
                ),
                "generated_reply_audit_flags": generated_audit_flags,
                "customer_visible_audit_flags": visible_audit_flags,
                "audit_flags": sorted(set(
                    generated_audit_flags + visible_audit_flags
                )),
                **({
                    "budget_retrieval_check": budget_retrieval_check,
                    "budget_fallback_check": budget_fallback_check,
                } if budget_retrieval_check is not None else {}),
                "note": (
                    "Provider/model identify the raw reply source, including a "
                    "provider fallback; customer-visible reply uses the existing "
                    "formatter when generation is ungrounded or unsuccessful."
                ),
                **({"retrieval_error": error} if error else {}),
            })
    finally:
        generation._gemini_generate = original_generate
    return results


def main() -> int:
    args = _parser().parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from dotenv import load_dotenv
        load_dotenv(repo_root / ".env")
        from commerce_adapters import ShopifyAdapter, get_adapter_for_client
        from tools import recommend_products, search_products
        if args.real_ai:
            from utils import get_ai_provider
    except ModuleNotFoundError as error:
        print(f"ERROR: required module unavailable ({error.name or 'unknown'})")
        return 2
    except Exception as error:
        print(f"ERROR: unable to initialize evaluation ({type(error).__name__})")
        return 2

    adapter = get_adapter_for_client(args.client_id)
    if not isinstance(adapter, ShopifyAdapter):
        print("ERROR: supplied client has no active Shopify inventory connection")
        return 2

    if args.real_ai:
        import models

        client = models.get_client_by_id(args.client_id)
        raw_settings = (client or {}).get("branding_settings")
        if isinstance(raw_settings, str):
            raw_settings = json.loads(raw_settings or "{}")
        bot_settings = (raw_settings or {}).get("bot_settings", {})
        if not isinstance(bot_settings, dict) or (
            bot_settings.get("product_recommendation_routing_enabled") is not True
        ):
            print(
                "ERROR: client product_recommendation_routing_enabled is not true; "
                "enable it before evaluating live recommendations."
            )
            return 2
        provider = get_ai_provider()
        key_name = (
            "OPENROUTER_API_KEY" if provider == "openrouter" else "GEMINI_API_KEY"
        )
        if not os.environ.get(key_name, "").strip():
            print(
                f"ERROR: provider {provider!r} is selected but {key_name} is missing."
            )
            return 2

    cases = [
        (
            "exact product title",
            "The Complete Snowboard",
            search_products,
            lambda result: result.get("success") and any(
                p.get("title") == "The Complete Snowboard" for p in result.get("products", [])
            ),
        ),
        (
            "tag and product-type retrieval",
            "winter snowboard",
            recommend_products,
            lambda result: result.get("success") and any(
                p.get("title") == "The Complete Snowboard"
                and "Winter" in (p.get("tags") or [])
                for p in result.get("products", [])
            ),
        ),
        (
            "vendor retrieval",
            "Snowboard Vendor",
            search_products,
            lambda result: result.get("success") and any(
                p.get("vendor") == "Snowboard Vendor" for p in result.get("products", [])
            ),
        ),
        (
            "active products only",
            "archived snowboard",
            search_products,
            lambda result: result.get("success") and all(
                p.get("title") != "The Archived Snowboard" for p in result.get("products", [])
            ) and bool(result.get("products")),
        ),
        (
            "out-of-stock variant preserved",
            "The Out of Stock Snowboard",
            recommend_products,
            lambda result: result.get("success") and any(
                p.get("title") == "The Out of Stock Snowboard" and not p.get("available")
                for p in result.get("products", [])
            ),
        ),
        (
            "available but untracked variant preserved",
            "The Inventory Not Tracked Snowboard",
            search_products,
            lambda result: result.get("success") and any(
                p.get("title") == "The Inventory Not Tracked Snowboard"
                and p.get("available") and p.get("quantity") == 0
                for p in result.get("products", [])
            ),
        ),
        (
            "description-only term limitation",
            "SUPERDUPER",
            recommend_products,
            lambda result: result.get("success") and not result.get("products"),
        ),
    ]

    results = []
    for label, query, function, check in cases:
        argument = "customer_need" if function is recommend_products else "query"
        result = _run_case(label, args.client_id, query, argument, function, check)
        if label == "description-only term limitation":
            result["status"] = "LIMITATION" if result["status"] == "PASS" else result["status"]
            result["note"] = (
                "Shopify's tested product search did not match description text; "
                "this is recorded, not counted as a passing capability."
            )
        results.append(result)
        print(json.dumps(result, ensure_ascii=True, default=str))

    if args.real_ai:
        try:
            live_results = _run_real_generation_cases(
                args.client_id,
                adapter,
                model_override=args.model,
            )
        except Exception as error:
            print(
                json.dumps({
                    "real_ai_error": f"{type(error).__name__}: {error}",
                    "configured_provider": get_ai_provider(),
                })
            )
            return 2
        for result in live_results:
            print(json.dumps(result, ensure_ascii=True, default=str))
        mock_results = []
    else:
        mock_results = _run_mock_generation_cases()
        for result in mock_results:
            print(json.dumps(result, ensure_ascii=True))

    passed = sum(result["status"] == "PASS" for result in results)
    failed = sum(result["status"] == "FAIL" for result in results)
    limitations = sum(result["status"] == "LIMITATION" for result in results)
    mock_failed = sum(result["status"] == "FAIL" for result in mock_results)
    real_flagged = (
        sum(bool(result["audit_flags"]) for result in live_results)
        if args.real_ai else 0
    )
    generated_count = (
        sum(bool(result.get("raw_model_reply_verbatim")) for result in live_results)
        if args.real_ai else 0
    )
    provider_503_count = (
        sum(result.get("provider_error_status") == "503" for result in live_results)
        if args.real_ai else 0
    )
    provider_429_count = (
        sum(result.get("provider_error_status") == "429" for result in live_results)
        if args.real_ai else 0
    )
    real_contract_failed = (
        sum(
            check.get("status") == "FAIL"
            for result in live_results
            for check in (
                result.get("budget_retrieval_check"),
                result.get("budget_fallback_check"),
            )
            if check is not None
        )
        if args.real_ai else 0
    )
    print(json.dumps({
        "summary": {
            "passed": passed,
            "failed": failed,
            "limitations": limitations,
            "total": len(results),
            "source": "real dev-store Shopify API",
        },
        "mock_only": {
            "passed": len(mock_results) - mock_failed,
            "failed": mock_failed,
            "total": len(mock_results),
        },
        **({
            "real_ai": {
                "generation_attempts": len(live_results),
                "generated_replies": generated_count,
                "provider_503_errors": provider_503_count,
                "provider_503_rate": (
                    round(provider_503_count / len(live_results), 3)
                    if live_results else 0
                ),
                "provider_429_quota_errors": provider_429_count,
                "provider_failure_rate": (
                    round(
                        (provider_503_count + provider_429_count)
                        / len(live_results),
                        3,
                    )
                    if live_results else 0
                ),
                "flagged_cases": real_flagged,
                "contract_failed": real_contract_failed,
                "source": "live provider responses and live dev-store retrieval",
            }
        } if args.real_ai else {}),
    }))
    return 1 if failed or mock_failed or real_contract_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
