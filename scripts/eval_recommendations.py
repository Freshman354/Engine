"""Evaluate live product search/recommendation retrieval against a Shopify client."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", required=True)
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
    from pipeline.stages.intent import _build_tool_response_text
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
            "scenario": "products array is additive and omitted for ordinary answers",
            "status": (
                "PASS"
                if contract_result.get("products") == products
                and "products" not in unrelated_result
                else "FAIL"
            ),
            "execution": "mock-only",
        },
    ]


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

    mock_results = _run_mock_generation_cases()
    for result in mock_results:
        print(json.dumps(result, ensure_ascii=True))

    passed = sum(result["status"] == "PASS" for result in results)
    failed = sum(result["status"] == "FAIL" for result in results)
    limitations = sum(result["status"] == "LIMITATION" for result in results)
    mock_failed = sum(result["status"] == "FAIL" for result in mock_results)
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
    }))
    return 1 if failed or mock_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
