"""Provider discrimination must preserve typed options and wire values."""

import pytest

from antfly.client_generated.models import (
    BraveSearchConfig,
    ChatToolsConfig,
    ExaSearchConfig,
    LinkupSearchConfig,
    SerperSearchConfig,
    TavilySearchConfig,
    VertexSearchConfig,
    YouSearchConfig,
)


@pytest.mark.parametrize(
    "provider,model,options",
    [
        ("exa", ExaSearchConfig, {"search_type": "neural", "num_results": 3}),
        ("serper", SerperSearchConfig, {"search_type": "news", "time_period": "w"}),
        ("tavily", TavilySearchConfig, {"search_depth": "advanced", "include_answer": False}),
        ("brave", BraveSearchConfig, {"freshness": "pw", "spellcheck": False}),
        ("you", YouSearchConfig, {"endpoint": "https://example.com/search"}),
        ("linkup", LinkupSearchConfig, {"depth": "deep", "output_type": "searchResults"}),
        ("vertex", VertexSearchConfig, {"project_id": "test-project", "data_store": "test-store"}),
    ],
)
def test_web_search_provider_discriminator_round_trip(provider, model, options):
    raw = {"provider": provider, **options}
    tools = ChatToolsConfig.from_dict({"web_search_config": raw})
    config = tools.web_search_config
    assert type(config) is model
    for field, value in options.items():
        assert getattr(config, field) == value
    assert tools.to_dict()["web_search_config"] == raw


def test_web_search_provider_discriminator_rejects_unknown_provider():
    with pytest.raises(ValueError):
        ChatToolsConfig.from_dict({"web_search_config": {"provider": "unknown"}})


@pytest.mark.parametrize(
    "provider,model",
    [
        ("exa", ExaSearchConfig),
        ("serper", SerperSearchConfig),
        ("tavily", TavilySearchConfig),
        ("brave", BraveSearchConfig),
        ("you", YouSearchConfig),
        ("linkup", LinkupSearchConfig),
        ("vertex", VertexSearchConfig),
    ],
)
def test_inline_constructor_preserves_omission_and_explicit_overrides(provider, model):
    discriminator = model.from_dict({"provider": provider}).provider
    config = model(provider=discriminator)
    tools = ChatToolsConfig(web_search_connection="production-search", web_search_config=config)
    assert tools.to_dict()["web_search_config"] == {"provider": provider}

    # False must be sent when explicitly requested, but never synthesized.
    config.include_content = False
    config.include_highlights = True
    config.safe_search = False
    config.max_results = 12
    config.timeout_ms = 20000
    assert tools.to_dict()["web_search_config"] == {
        "provider": provider,
        "include_content": False,
        "include_highlights": True,
        "safe_search": False,
        "max_results": 12,
        "timeout_ms": 20000,
    }


def test_exa_count_override_does_not_materialize_other_defaults():
    from antfly.client_generated.models import ExaSearchConfigProvider

    config = ExaSearchConfig(provider=ExaSearchConfigProvider.EXA, num_results=12)
    tools = ChatToolsConfig(web_search_connection="production-search", web_search_config=config)
    assert tools.to_dict()["web_search_config"] == {"provider": "exa", "num_results": 12}
