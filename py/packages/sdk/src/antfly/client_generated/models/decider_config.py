from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.decider_config_decision_method import DeciderConfigDecisionMethod
from ..models.decider_config_provider import DeciderConfigProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.decider_config_embedding_options import DeciderConfigEmbeddingOptions
    from ..models.rate_limit_config import RateLimitConfig


T = TypeVar("T", bound="DeciderConfig")


@_attrs_define
class DeciderConfig:
    """
    Attributes:
        provider (DeciderConfigProvider):
        decision_method (DeciderConfigDecisionMethod | Unset): Explicit result contract. embedding_similarity is
            available with Antfly EmbeddingGemma 2 choice and multi_choice questions; rejects score and predicate requests
            and returns raw cosine similarities. Default: DeciderConfigDecisionMethod.TYPED.
        model (str | Unset): Required for Antfly and OpenAI; Jev defaults to jev-latest.
        model_identity (str | Unset): Pin the exact EmbeddingGemma 2 assets and recipe. Requires embedding_similarity.
        embedding_options (DeciderConfigEmbeddingOptions | Unset): EmbeddingGemma 2 geometry and acceptance defaults.
            Acceptance is serialized onto each question and can be overridden per question. Other models reject these
            options. Raw cosine similarities never represent probabilities.
        url (str | Unset): Provider base URL; endpoint path is selected by provider. OpenAI defaults to
            https://api.openai.com/v1 and appends /decisions.
        api_key (str | Unset): API key or secret reference. Defaults to the provider environment variable.
        max_rows (int | Unset):  Default: 10000.
        max_input_tokens (int | Unset):  Default: 1000000.
        rate_limit (RateLimitConfig | Unset): Outbound provider limits shared within one Antfly process by effective
            endpoint, operation, model, credential source, project and region/location.
            Conflicting policies for an active scope are rejected. These limits do
            not coordinate across replicas or infer the provider's account quota.
        batch_size (int | Unset):  Default: 32.
    """

    provider: DeciderConfigProvider
    decision_method: DeciderConfigDecisionMethod | Unset = DeciderConfigDecisionMethod.TYPED
    model: str | Unset = UNSET
    model_identity: str | Unset = UNSET
    embedding_options: DeciderConfigEmbeddingOptions | Unset = UNSET
    url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    max_rows: int | Unset = 10000
    max_input_tokens: int | Unset = 1000000
    rate_limit: RateLimitConfig | Unset = UNSET
    batch_size: int | Unset = 32

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        decision_method: str | Unset = UNSET
        if not isinstance(self.decision_method, Unset):
            decision_method = self.decision_method.value

        model = self.model

        model_identity = self.model_identity

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        url = self.url

        api_key = self.api_key

        max_rows = self.max_rows

        max_input_tokens = self.max_input_tokens

        rate_limit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.rate_limit, Unset):
            rate_limit = self.rate_limit.to_dict()

        batch_size = self.batch_size

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "provider": provider,
            }
        )
        if decision_method is not UNSET:
            field_dict["decision_method"] = decision_method
        if model is not UNSET:
            field_dict["model"] = model
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if embedding_options is not UNSET:
            field_dict["embedding_options"] = embedding_options
        if url is not UNSET:
            field_dict["url"] = url
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if max_rows is not UNSET:
            field_dict["max_rows"] = max_rows
        if max_input_tokens is not UNSET:
            field_dict["max_input_tokens"] = max_input_tokens
        if rate_limit is not UNSET:
            field_dict["rate_limit"] = rate_limit
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.decider_config_embedding_options import DeciderConfigEmbeddingOptions
        from ..models.rate_limit_config import RateLimitConfig

        d = dict(src_dict)
        provider = DeciderConfigProvider(d.pop("provider"))

        _decision_method = d.pop("decision_method", UNSET)
        decision_method: DeciderConfigDecisionMethod | Unset
        if isinstance(_decision_method, Unset):
            decision_method = UNSET
        else:
            decision_method = DeciderConfigDecisionMethod(_decision_method)

        model = d.pop("model", UNSET)

        model_identity = d.pop("model_identity", UNSET)

        _embedding_options = d.pop("embedding_options", UNSET)
        embedding_options: DeciderConfigEmbeddingOptions | Unset
        if isinstance(_embedding_options, Unset):
            embedding_options = UNSET
        else:
            embedding_options = DeciderConfigEmbeddingOptions.from_dict(_embedding_options)

        url = d.pop("url", UNSET)

        api_key = d.pop("api_key", UNSET)

        max_rows = d.pop("max_rows", UNSET)

        max_input_tokens = d.pop("max_input_tokens", UNSET)

        _rate_limit = d.pop("rate_limit", UNSET)
        rate_limit: RateLimitConfig | Unset
        if isinstance(_rate_limit, Unset):
            rate_limit = UNSET
        else:
            rate_limit = RateLimitConfig.from_dict(_rate_limit)

        batch_size = d.pop("batch_size", UNSET)

        decider_config = cls(
            provider=provider,
            decision_method=decision_method,
            model=model,
            model_identity=model_identity,
            embedding_options=embedding_options,
            url=url,
            api_key=api_key,
            max_rows=max_rows,
            max_input_tokens=max_input_tokens,
            rate_limit=rate_limit,
            batch_size=batch_size,
        )

        return decider_config
