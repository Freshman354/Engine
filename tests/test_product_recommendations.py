"""Deterministic checks for commerce product retrieval and ranking."""

import os
from types import SimpleNamespace
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "postgresql://unused:unused@localhost:5432/unused")

from commerce_adapters import InventoryMatch, _shopify_product_search_query
from commerce_adapters import InventoryResult
from pipeline.stages.generation import generate_product_recommendation_response
from pipeline.stages.intent import _build_tool_response_text
from pipeline.context import PipelineResult
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


def test_generated_recommendation_must_name_a_retrieved_product():
    products = [{
        "title": "The Complete Snowboard",
        "description": "A premium snowboard.",
        "available": True,
        "price": "699.95",
    }]
    with patch(
        "pipeline.stages.generation._gemini_generate",
        return_value=SimpleNamespace(
            text="The Complete Snowboard is available for 699.95."
        ),
    ) as generate:
        response = generate_product_recommendation_response(
            "Find me a winter snowboard", products, model=object(),
        )

    assert response == "The Complete Snowboard is available for 699.95."
    assert "The Complete Snowboard" in generate.call_args.args[1]


def test_unanchored_generation_uses_formatter_fallback():
    products = [{"title": "The Complete Snowboard", "available": True, "price": "699.95"}]
    with patch(
        "pipeline.stages.generation._gemini_generate",
        return_value=SimpleNamespace(text="Try our all-new Alpine Pro 9000."),
    ):
        response = generate_product_recommendation_response(
            "Find me a snowboard", products, model=object(),
        )

    assert response is None
    fallback = _build_tool_response_text("recommend_products", {
        "products": products,
    })
    assert "The Complete Snowboard" in fallback
    assert "Alpine Pro 9000" not in fallback


def test_generation_is_skipped_when_model_or_products_are_missing():
    with patch("pipeline.stages.generation._gemini_generate") as generate:
        assert generate_product_recommendation_response("need", [], object()) is None
        assert generate_product_recommendation_response(
            "need", [{"title": "Item"}], None,
        ) is None

    generate.assert_not_called()


def test_recommendation_formatter_asks_for_clarification_on_no_match():
    response = _build_tool_response_text("recommend_products", {"products": []})

    assert "preferred product type or budget" in response
    assert "No products found" not in response


def test_pipeline_result_adds_products_only_when_available():
    products = [{"title": "The Complete Snowboard"}]
    with_products = PipelineResult(
        response="Here is an option.",
        method="product_recommendation",
        confidence=0.9,
        products=products,
    ).to_dict()
    without_products = PipelineResult(
        response="I can help with that.",
        method="rag",
        confidence=0.8,
    ).to_dict()

    assert with_products["products"] == products
    assert "products" not in without_products
