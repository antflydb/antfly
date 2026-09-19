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
