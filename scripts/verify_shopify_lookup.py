"""Verify Shopify product lookup for one Lumvi tenant.

Shopify credentials are obtained only through
commerce_adapters.get_adapter_for_client(client_id), the resolver used by the
/api/chat product-search path. This script never reads Shopify credentials from
environment variables or prints credentials.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict
from itertools import combinations
from pathlib import Path
from typing import Any

_HARMLESS_IMPORT_PROBES = {
    "aioquic", "backports", "brotli", "brotlicffi", "chardet", "fcntl",
    "h2", "posix", "simplejson", "socks", "wmi",
}


CATALOG_QUERY = """
query VerifyProductCatalog {
  products(first: 50) {
    edges {
      node {
        title
        variants(first: 10) {
          edges {
            node {
              title
              availableForSale
              selectedOptions { name value }
            }
          }
        }
      }
    }
  }
}
"""

LIVE_INVENTORY_SCOPE_QUERY = """
query VerifyLiveInventoryScope {
    currentAppInstallation {
        accessScopes { handle }
    }
    inventoryItems(first: 1) {
        edges {
            node {
                inventoryLevels(first: 1) {
                    edges { node { id } }
                }
            }
        }
    }
}
"""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--client-id",
        required=True,
        help="Lumvi client whose stored Shopify connection should be tested",
    )
    parser.add_argument(
        "--compare-client-id",
        help="Optional second Lumvi client for live cross-tenant isolation checks",
    )
    return parser


def _normalise(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value).casefold()).strip()


def _options(variant: dict[str, Any]) -> dict[str, str]:
    return {
        str(option.get("name", "")).strip().lower(): str(option.get("value", "")).strip()
        for option in variant.get("selectedOptions", [])
        if option.get("name") and option.get("value")
    }


def _catalog_products(payload: dict[str, Any]) -> list[dict[str, Any]]:
    edges = (((payload.get("data") or {}).get("products") or {}).get("edges") or [])
    return [edge.get("node", {}) for edge in edges]


def _variants(product: dict[str, Any]) -> list[dict[str, Any]]:
    edges = (((product.get("variants") or {}).get("edges")) or [])
    return [edge.get("node", {}) for edge in edges]


def _scenario_fixtures(products: list[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    fixtures: dict[str, dict[str, Any] | None] = {
        "exact variant match": None,
        "out-of-stock variant": None,
        "single-variant product": None,
        'ambiguous request "the blue one"': None,
        "nonexistent combination": None,
    }

    for product in products:
        title = product.get("title", "").strip()
        variants = _variants(product)
        option_rows = [(variant, _options(variant)) for variant in variants]

        if fixtures["exact variant match"] is None and len(variants) > 1:
            for variant, options in option_rows:
                values = list(options.values())
                if values and sum(_options(other) == options for other in variants) == 1:
                    fixtures["exact variant match"] = {
                        "product": product,
                        "variant": variant,
                        "options": options,
                        "request": f"{title} {' '.join(values)}",
                    }
                    break

        if fixtures["out-of-stock variant"] is None:
            for variant, options in option_rows:
                if options and not variant.get("availableForSale", True):
                    fixtures["out-of-stock variant"] = {
                        "product": product,
                        "variant": variant,
                        "options": options,
                        "request": f"{title} {' '.join(options.values())}",
                    }
                    break

        if fixtures["single-variant product"] is None and len(variants) == 1:
            fixtures["single-variant product"] = {
                "product": product,
                "variant": variants[0],
                "options": _options(variants[0]),
                "request": title,
            }

        if fixtures['ambiguous request "the blue one"'] is None:
            blue_variants = [
                variant for variant, options in option_rows
                if any(_normalise(value) == "blue" for value in options.values())
            ]
            if len(blue_variants) > 1:
                fixtures['ambiguous request "the blue one"'] = {
                    "product": product,
                    "variants": blue_variants,
                    "request": "the blue one",
                }

        if fixtures["nonexistent combination"] is None:
            option_names = sorted({name for _, options in option_rows for name in options})
            for first_name, second_name in combinations(option_names, 2):
                first_values = sorted({options[first_name] for _, options in option_rows if first_name in options})
                second_values = sorted({options[second_name] for _, options in option_rows if second_name in options})
                present = {
                    (options.get(first_name), options.get(second_name))
                    for _, options in option_rows
                    if first_name in options and second_name in options
                }
                missing = next(
                    ((first, second) for first in first_values for second in second_values
                     if (first, second) not in present),
                    None,
                )
                if missing:
                    fixtures["nonexistent combination"] = {
                        "product": product,
                        "request": f"{title} {missing[0]} {missing[1]}",
                        "expected_options": {first_name: missing[0], second_name: missing[1]},
                        "expected_matching_option": {first_name: missing[0]},
                    }
                    break

        if all(fixtures.values()):
            break

    return fixtures


def _redact(value: Any, secret_values: list[str]) -> Any:
    if isinstance(value, dict):
        return {key: _redact(item, secret_values) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item, secret_values) for item in value]
    if isinstance(value, str):
        for secret in secret_values:
            if secret:
                value = value.replace(secret, "[REDACTED]")
    return value


def _emit(payload: dict[str, Any], secret_values: list[str]) -> None:
    print(json.dumps(_redact(payload, secret_values), indent=2, ensure_ascii=True, default=str))


def _fetch_catalog(adapter: Any, secret_values: list[str]) -> list[dict[str, Any]]:
    import requests

    try:
        access_token = adapter._get_access_token()
    except Exception as error:
        raise RuntimeError(f"Shopify authentication failed ({type(error).__name__}); details omitted") from None

    secret_values.append(access_token)
    url = f"https://{adapter.shop_domain}/admin/api/{adapter.API_VERSION}/graphql.json"
    try:
        response = requests.post(
            url,
            json={"query": CATALOG_QUERY},
            headers={"X-Shopify-Access-Token": access_token},
            timeout=15,
        )
    except requests.exceptions.RequestException as error:
        raise RuntimeError(f"Shopify catalog request failed ({type(error).__name__}); details omitted") from None

    if response.status_code != 200:
        raise RuntimeError(f"Shopify catalog request returned HTTP {response.status_code}")
    payload = response.json()
    if payload.get("errors"):
        messages = "; ".join(str(error.get("message", "GraphQL error")) for error in payload["errors"])
        raise RuntimeError(_redact(messages, secret_values))
    return _catalog_products(payload)


def _capture_lookup(client_id: str, product_query: str, fresh: bool) -> tuple[Any, list[dict[str, Any] | None]]:
    import requests
    import commerce_adapters

    raw_responses: list[dict[str, Any] | None] = []
    original_post = requests.post

    def capture_post(url: str, *args: Any, **kwargs: Any) -> Any:
        response = original_post(url, *args, **kwargs)
        if "/graphql.json" in str(url):
            try:
                raw_responses.append({"http_status": response.status_code, "body": response.json()})
            except (ValueError, AttributeError):
                raw_responses.append({"http_status": response.status_code, "body": "<non-JSON response>"})
        return response

    with __import__("unittest.mock", fromlist=["patch"]).patch.object(requests, "post", side_effect=capture_post):
        result = commerce_adapters.search_inventory_cached(
            client_id,
            product_query,
            fresh=fresh,
        )
    return result, raw_responses


def _same_product_matches(result: Any, product_title: str) -> list[Any]:
    expected = _normalise(product_title)
    return [match for match in result.matches if _normalise(match.title) == expected]


def _run_case(
    label: str,
    client_id: str,
    product: dict[str, Any],
    customer_request: str,
    secret_values: list[str],
    check,
) -> bool:
    from tools import _select_product_variants

    title = product["title"]
    result, raw_responses = _capture_lookup(client_id, title, fresh=True)
    candidates = _same_product_matches(result, title) if result.resolved else []
    selected, needs_clarification, response_state = _select_product_variants(candidates, customer_request)
    passed = bool(check(result, selected, needs_clarification, response_state))
    _emit({
        "scenario": label,
        "status": "PASS" if passed else "FAIL",
        "customer_request": customer_request,
        "raw_shopify_response": raw_responses[-1] if raw_responses else None,
        "adapter_output": asdict(result),
        "post_search_selection": {
            "variants": [asdict(match) for match in selected],
            "clarification_required": needs_clarification,
            "response_state": response_state,
        },
    }, secret_values)
    return passed


def _print_skip(label: str, reason: str, secret_values: list[str]) -> None:
    _emit({"scenario": label, "status": "SKIP", "reason": reason}, secret_values)


def _run_missing_scope_case(client_id: str, adapter: Any, secret_values: list[str]) -> bool:
    import commerce_adapters
    from unittest.mock import patch

    original_scopes = adapter.shopify_scopes
    adapter.shopify_scopes = {"read_products"}
    try:
        with patch.object(commerce_adapters, "get_adapter_for_client", return_value=adapter):
            result = commerce_adapters.search_inventory_cached(
                client_id,
                "__missing_scope_probe__",
                fresh=True,
            )
    finally:
        adapter.shopify_scopes = original_scopes

    passed = not result.resolved and "scope" in (result.error or "").lower()
    _emit({
        "scenario": "local stored-scope guard",
        "status": "PASS" if passed else "FAIL",
        "raw_shopify_response": {
            "request_sent": False,
            "reason": "the adapter rejects the deliberately reduced scope set locally",
        },
        "adapter_output": asdict(result),
    }, secret_values)
    return passed


def _run_live_scope_case(adapter: Any, secret_values: list[str]) -> bool:
    import requests

    try:
        access_token = adapter._get_access_token()
        secret_values.append(access_token)
        response = requests.post(
            f"https://{adapter.shop_domain}/admin/api/{adapter.API_VERSION}/graphql.json",
            json={"query": LIVE_INVENTORY_SCOPE_QUERY},
            headers={"X-Shopify-Access-Token": access_token},
            timeout=15,
        )
        payload = response.json()
    except (requests.exceptions.RequestException, ValueError) as error:
        _emit({
            "scenario": "live Shopify read_inventory scope enforcement",
            "status": "FAIL",
            "reason": f"live Admin API probe failed ({type(error).__name__})",
        }, secret_values)
        return False
    except Exception as error:
        _emit({
            "scenario": "live Shopify read_inventory scope enforcement",
            "status": "FAIL",
            "reason": f"could not obtain a usable Shopify token ({type(error).__name__})",
        }, secret_values)
        return False

    data = payload.get("data") or {}
    installation = data.get("currentAppInstallation") or {}
    scopes = {
        scope.get("handle", "")
        for scope in installation.get("accessScopes") or []
    }
    has_read_inventory = "read_inventory" in scopes
    errors = payload.get("errors") or []
    denial_codes = {"ACCESS_DENIED", "FORBIDDEN", "UNAUTHORIZED"}
    inventory_denied = response.status_code in (401, 403) or any(
        str((error.get("extensions") or {}).get("code", "")).upper() in denial_codes
        or any(term in str(error.get("message", "")).lower()
               for term in ("access denied", "insufficient scope", "missing scope"))
        for error in errors
    )
    scope_state_known = "currentAppInstallation" in data and "accessScopes" in installation
    passed = scope_state_known and (
        (has_read_inventory and response.status_code == 200 and not inventory_denied and not errors)
        or (not has_read_inventory and inventory_denied)
    )
    inventory_result = "denied" if inventory_denied else "allowed"
    _emit({
        "scenario": "live Shopify read_inventory scope enforcement",
        "status": "PASS" if passed else "FAIL",
        "shopify_reports_read_inventory": has_read_inventory,
        "inventory_levels_api": inventory_result,
        "http_status": response.status_code,
        "graphql_errors": [str(error.get("message", "GraphQL error")) for error in errors],
    }, secret_values)
    return passed


def _cache_key(client_id: str, query: str) -> str:
    return f"{client_id}:{query.lower().strip()}:"


def _run_isolation_check(
    client_a_id: str,
    adapter_a: Any,
    products_a: list[dict[str, Any]],
    client_b_id: str,
    adapter_b: Any,
    products_b: list[dict[str, Any]],
    secret_values: list[str],
) -> bool | None:
    import commerce_adapters

    titles_a = {_normalise(product.get("title", "")) for product in products_a}
    unique_product = next(
        (product for product in products_b
         if _normalise(product.get("title", "")) not in titles_a),
        None,
    )
    if unique_product is None:
        _print_skip(
            "cross-tenant product and cache isolation",
            "no product title unique to Client B was found in the first 50 catalog products",
            secret_values,
        )
        return None

    query = unique_product["title"]
    key_a = _cache_key(client_a_id, query)
    key_b = _cache_key(client_b_id, query)
    commerce_adapters._INVENTORY_CACHE.pop(key_a, None)
    commerce_adapters._INVENTORY_CACHE.pop(key_b, None)

    result_a, raw_a = _capture_lookup(client_a_id, query, fresh=True)
    result_b, raw_b = _capture_lookup(client_b_id, query, fresh=False)
    found_a = any(_normalise(match.title) == _normalise(query) for match in result_a.matches)
    found_b = any(_normalise(match.title) == _normalise(query) for match in result_b.matches)
    separate_cache_entries = (
        key_a != key_b
        and key_a in commerce_adapters._INVENTORY_CACHE
        and key_b in commerce_adapters._INVENTORY_CACHE
    )
    passed = (
        result_a.resolved
        and result_b.resolved
        and not found_a
        and found_b
        and separate_cache_entries
        and bool(raw_b)
    )
    _emit({
        "scenario": "cross-tenant product and cache isolation",
        "status": "PASS" if passed else "FAIL",
        "query_for_client_b_unique_product": query,
        "client_a_raw_shopify_response": raw_a[-1] if raw_a else None,
        "client_a_adapter_output": asdict(result_a),
        "client_b_raw_shopify_response": raw_b[-1] if raw_b else None,
        "client_b_adapter_output": asdict(result_b),
        "cache_keys_are_distinct_and_populated": separate_cache_entries,
        "client_a_returned_client_b_product": found_a,
        "client_b_returned_own_product": found_b,
    }, secret_values)
    return passed


def main() -> int:
    args = _parser().parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    try:
        from dotenv import load_dotenv
        load_dotenv()
        import commerce_adapters
        from commerce_adapters import ShopifyAdapter, get_adapter_for_client
    except ModuleNotFoundError as error:
        if error.name in _HARMLESS_IMPORT_PROBES:
            pass
        else:
            print(
                "ERROR: unable to initialize application modules "
                f"(missing module: {error.name or 'unknown'})"
            )
            return 2
    except Exception as error:
        print(f"ERROR: unable to initialize application modules ({type(error).__name__})")
        return 2

    adapter_a = get_adapter_for_client(args.client_id)
    if not isinstance(adapter_a, ShopifyAdapter):
        print("ERROR: --client-id has no active Shopify inventory connection")
        return 2

    secrets_a = [adapter_a._static_access_token, adapter_a.shopify_client_secret]
    try:
        products_a = _fetch_catalog(adapter_a, secrets_a)
    except Exception as error:
        print(f"ERROR: Client A catalog discovery failed: {error}")
        return 2

    print(f"Client {args.client_id}: discovered {len(products_a)} products; Shopify credentials were resolved from this client's database integration.")
    fixtures = _scenario_fixtures(products_a)
    passed = failed = skipped = 0

    checks = {
        "exact variant match": lambda result, selected, clarify, state, fixture: (
            result.resolved and len(selected) == 1
            and selected[0].options == fixture["options"]
            and not clarify and state == "matched"
        ),
        "out-of-stock variant": lambda result, selected, clarify, state, fixture: (
            result.resolved and len(selected) == 1
            and not selected[0].available
            and selected[0].options == fixture["options"]
        ),
        "single-variant product": lambda result, selected, clarify, state, fixture: (
            result.resolved and len(selected) == 1 and not clarify and state == "matched"
        ),
        'ambiguous request "the blue one"': lambda result, selected, clarify, state, fixture: (
            result.resolved and len(selected) > 1 and clarify
            and state == "ambiguous"
            and all(any(_normalise(value) == "blue" for value in match.options.values()) for match in selected)
        ),
        "nonexistent combination": lambda result, selected, clarify, state, fixture: (
            result.resolved and bool(selected) and not clarify
            and state == "combination_not_found"
            and all(
                all(match.options.get(name) == value for name, value in fixture["expected_matching_option"].items())
                for match in selected
            )
        ),
    }

    for label, fixture in fixtures.items():
        if fixture is None:
            _print_skip(label, "the connected store's discovered catalog has no suitable product/variant fixture", secrets_a)
            skipped += 1
            continue
        product = fixture["product"]
        ok = _run_case(
            label,
            args.client_id,
            product,
            fixture["request"],
            secrets_a,
            lambda result, selected, clarify, state, fixture=fixture, label=label: checks[label](
                result, selected, clarify, state, fixture
            ),
        )
        passed += int(ok)
        failed += int(not ok)

    scope_ok = _run_missing_scope_case(args.client_id, adapter_a, secrets_a)
    passed += int(scope_ok)
    failed += int(not scope_ok)

    live_scope_ok = _run_live_scope_case(adapter_a, secrets_a)
    passed += int(live_scope_ok)
    failed += int(not live_scope_ok)

    if args.compare_client_id:
        adapter_b = get_adapter_for_client(args.compare_client_id)
        if not isinstance(adapter_b, ShopifyAdapter):
            print("ERROR: --compare-client-id has no active Shopify inventory connection")
            return 2
        secrets_b = [adapter_b._static_access_token, adapter_b.shopify_client_secret]
        try:
            products_b = _fetch_catalog(adapter_b, secrets_b)
        except Exception as error:
            print(f"ERROR: Client B catalog discovery failed ({type(error).__name__}); details omitted")
            return 2
        secrets_a.extend(secrets_b)
        isolation = _run_isolation_check(
            args.client_id,
            adapter_a,
            products_a,
            args.compare_client_id,
            adapter_b,
            products_b,
            secrets_a,
        )
        if isolation is None:
            skipped += 1
        else:
            passed += int(isolation)
            failed += int(not isolation)
    else:
        _emit({
            "scenario": "tenant isolation code review",
            "status": "REVIEWED",
            "findings": [
                "get_adapter_for_client(client_id) delegates to _get_inventory_integration(client_id)",
                "_get_inventory_integration calls list_integrations(client_id, redact=False), whose SQL filters WHERE client_id = %s",
                "search_inventory_cached prefixes its key with client_id",
            ],
            "runtime_note": "pass --compare-client-id to exercise two live stores",
        }, secrets_a)

    print(f"\nSummary: {passed} passed, {failed} failed, {skipped} skipped")
    return 1 if failed else (2 if skipped else 0)


if __name__ == "__main__":
    raise SystemExit(main())
