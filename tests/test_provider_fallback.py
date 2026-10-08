from types import SimpleNamespace
from unittest.mock import patch, MagicMock
import sys

import pytest

import utils
from pipeline.stages.generation import generate_product_recommendation_response


class _FailingGeminiModel:
    def generate_content(self, **kwargs):
        raise RuntimeError("503 Service Unavailable")


class _RaisingGeminiModel:
    def __init__(self, error):
        self.error = error

    def generate_content(self, **kwargs):
        raise self.error


def test_recommendation_generation_uses_groq_fallback_by_default():
    """Default behavior (GEMINI_FALLBACK_PROVIDER='groq'): Gemini quota error falls back to Groq."""
    response = SimpleNamespace(
        text="The Complete Snowboard is listed at $699.95.",
        provider="groq",
        model="llama-3.1-8b-instant",
    )
    products = [{
        "title": "The Complete Snowboard",
        "available": True,
        "price": "699.95",
    }]

    with patch.object(utils, "GEMINI_FALLBACK_PROVIDER", "groq"):
        with patch.object(utils, "_is_quota_error", return_value=True):
            # Mock the Groq client and its completion
            mock_groq = MagicMock()
            mock_groq.chat.completions.create.return_value = SimpleNamespace(
                choices=[SimpleNamespace(message=SimpleNamespace(content=response.text))]
            )
            with patch.object(utils, "_get_groq_client", return_value=mock_groq):
                with patch.object(
                    utils,
                    "_generate_openrouter_primary",
                    side_effect=RuntimeError("Should not be called"),
                ) as openrouter_generate:
                    reply = generate_product_recommendation_response(
                        "Find me a snowboard",
                        products,
                        _FailingGeminiModel(),
                    )

    assert reply == response.text
    openrouter_generate.assert_not_called()


def test_recommendation_generation_uses_openrouter_when_flag_set():
    """When GEMINI_FALLBACK_PROVIDER='openrouter': Gemini quota error tries OpenRouter first."""
    response = SimpleNamespace(
        text="The Complete Snowboard is listed at $699.95.",
        provider="openrouter",
        model="meta-llama/llama-4-maverick",
    )
    products = [{
        "title": "The Complete Snowboard",
        "available": True,
        "price": "699.95",
    }]

    with patch.object(utils, "GEMINI_FALLBACK_PROVIDER", "openrouter"):
        with patch.object(utils, "_is_quota_error", return_value=True):
            with patch.object(
                utils,
                "_generate_openrouter_primary",
                return_value=response,
            ) as openrouter_generate:
                reply = generate_product_recommendation_response(
                    "Find me a snowboard",
                    products,
                    _FailingGeminiModel(),
                )

    assert reply == response.text
    openrouter_generate.assert_called_once()
    assert "The Complete Snowboard" in openrouter_generate.call_args.args[0]


def test_gemini_fallback_tries_groq_after_openrouter_fails():
    """When GEMINI_FALLBACK_PROVIDER='openrouter' and OpenRouter fails, fall back to Groq."""
    gemini_error = RuntimeError("429 quota exhausted")
    groq_response = SimpleNamespace(
        text="Fallback to Groq success",
        provider="groq",
        model="llama-3.1-8b-instant",
    )
    
    with patch.object(utils, "GEMINI_FALLBACK_PROVIDER", "openrouter"):
        with patch.object(utils, "_is_quota_error", return_value=True):
            with patch.object(
                utils,
                "_generate_openrouter_primary",
                side_effect=RuntimeError("OpenRouter unavailable"),
            ):
                mock_groq = MagicMock()
                mock_groq.chat.completions.create.return_value = SimpleNamespace(
                    choices=[SimpleNamespace(message=SimpleNamespace(content=groq_response.text))]
                )
                with patch.object(utils, "_get_groq_client", return_value=mock_groq):
                    resp = utils._generate_gemini_primary(
                        _RaisingGeminiModel(gemini_error),
                        "recommend products",
                        "gemini-test-model",
                    )
    
    assert resp.provider == "groq"
    assert resp.text == groq_response.text


def test_failed_provider_fallback_reraises_original_gemini_error():
    """When all fallbacks fail, re-raise the original Gemini error."""
    gemini_error = RuntimeError("429 quota exhausted")
    
    # Mock the google.genai import to allow the function to proceed
    mock_genai_types = MagicMock()
    mock_genai = MagicMock()
    mock_genai.types = mock_genai_types
    
    with patch.dict(sys.modules, {'google.genai': mock_genai, 'google.genai.types': mock_genai_types}):
        with patch.object(utils, "GEMINI_FALLBACK_PROVIDER", "groq"):
            with patch.object(utils, "_is_quota_error", return_value=True):
                with patch.object(utils, "_get_groq_client", return_value=None):
                    # The model will raise the gemini_error when generate_content is called
                    with pytest.raises(RuntimeError, match="429 quota exhausted"):
                        utils._generate_gemini_primary(
                            _RaisingGeminiModel(gemini_error),
                            "recommend products",
                            "gemini-test-model",
                        )
