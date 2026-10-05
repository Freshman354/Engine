"""Deterministic checks for commerce product retrieval and ranking."""

import os

os.environ.setdefault("DATABASE_URL", "postgresql://unused:unused@localhost:5432/unused")

from commerce_adapters import InventoryMatch, _shopify_product_search_query
from commerce_adapters import InventoryResult
from tools import _catalog_search, _rank_product_matches


def test_shopify_query_uses_verified_fields_and_active_status():
    query = _shopify_product_search_query("winter snowboard")

    assert query.startswith("status:active AND (")
    assert "title:*winter*" in query
    assert 'tag:"winter"' in query
    assert 'product_type:"winter"' in query
    assert 'vendor:"winter"' in query
    assert "description:" not in query


def test_product_ranking_weights_title_over_metadata_and_stock():
    title_match = InventoryMatch(
        title="Winter Snowboard",
        available=False,
        description="A snowboard for winter",
    )
    tag_match = InventoryMatch(
        title="Cold Weather Board",
        available=True,
        tags=["Winter", "Snowboard"],
    )
    unrelated = InventoryMatch(title="Gift Card", available=True)

    ranked = _rank_product_matches(
        [tag_match, unrelated, title_match],
        "winter snowboard",
    )

    assert ranked == [title_match, tag_match]


def test_description_text_contributes_to_local_ranking():
    described = InventoryMatch(
        title="The Complete Snowboard",
        available=True,
        description="A premium SUPERDUPER winter board",
    )
    unrelated = InventoryMatch(
        title="Basic Snowboard",
        available=True,
        description="A simple product",
    )

    ranked = _rank_product_matches([unrelated, described], "SUPERDUPER")

    assert ranked == [described]


def test_vague_or_stopword_only_need_does_not_select_random_products():
    matches = [InventoryMatch(title="Gift Card", available=True)]

    assert _rank_product_matches(matches, "something for me") == []


def test_catalog_result_groups_variants_by_product():
    matches = [
        InventoryMatch(
            title="Winter Snowboard",
            available=True,
            variant="Blue",
            product_url="https://store.example/products/winter-snowboard",
            options={"color": "Blue"},
        ),
        InventoryMatch(
            title="Winter Snowboard",
            available=False,
            variant="Red",
            product_url="https://store.example/products/winter-snowboard",
            options={"color": "Red"},
        ),
    ]

    from unittest.mock import patch

    with patch(
        "commerce_adapters.search_inventory_cached",
        return_value=InventoryResult(resolved=True, matches=matches),
    ):
        result = _catalog_search("tenant-x", "Winter Snowboard", 5)

    assert result["success"] is True
    assert result["count"] == 1
    assert len(result["products"]) == 1
    assert result["products"][0]["available"] is True
    assert len(result["products"][0]["variants"]) == 2
