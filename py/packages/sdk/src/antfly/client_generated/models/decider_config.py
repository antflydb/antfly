from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.constrained_output import ConstrainedOutput
from ..models.constrained_probabilities import ConstrainedProbabilities
from ..models.decider_config_decision_method import DeciderConfigDecisionMethod
from ..models.decider_config_provider import DeciderConfigProvider
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.constrained_generation_batch import ConstrainedGenerationBatch
    from ..models.decider_config_embedding_options import DeciderConfigEmbeddingOptions
    from ..models.generator_config import GeneratorConfig
    from ..models.rate_limit_config import RateLimitConfig


T = TypeVar("T", bound="DeciderConfig")


@_attrs_define
class DeciderConfig:
    """
    Attributes:
        provider (DeciderConfigProvider | Unset):
        decision_method (DeciderConfigDecisionMethod | Unset): Explicit result contract. embedding_similarity is
            available with Antfly EmbeddingGemma 2 choice and multi_choice questions; rejects score and predicate requests
            and returns raw cosine similarities. Default: DeciderConfigDecisionMethod.TYPED.
        generator (GeneratorConfig | Unset): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        instructions (str | Unset):
        output (ConstrainedOutput | Unset): Provider and model admission selects a supported constrained mechanism. Auto
            prefers forced tools, then provider JSON schema, then native grammar. Free-text parsing is not a mode.
        probabilities (ConstrainedProbabilities | Unset): Token probabilities require complete decoder evidence for the
            allowed values. Verdict-only is explicit opt-in; model-reported confidence is never used.
        batch (ConstrainedGenerationBatch | Unset):
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
        max_image_input_tokens (int | Unset): Provider/model upper bound on the input-token contribution of one image.
            Required when sending images through typed hosted or generated deciders, including native generators and image
            URLs. The runtime reserves this allowance per image in addition to serialized non-image request bytes; actual
            usage can add shared quota debt. Generated deciders apply the allowance to the selected generator's shared
            quota. Omit for text-only deciders.
        rate_limit (RateLimitConfig | Unset): Outbound provider limits shared within one Antfly process by effective
            endpoint, operation, model, credential source, project and region/location.
            Conflicting policies for an active scope are rejected. These limits do
            not coordinate across replicas or infer the provider's account quota.
        batch_size (int | Unset):  Default: 32.
    """

    provider: DeciderConfigProvider | Unset = UNSET
    decision_method: DeciderConfigDecisionMethod | Unset = DeciderConfigDecisionMethod.TYPED
    generator: GeneratorConfig | Unset = UNSET
    instructions: str | Unset = UNSET
    output: ConstrainedOutput | Unset = UNSET
    probabilities: ConstrainedProbabilities | Unset = UNSET
    batch: ConstrainedGenerationBatch | Unset = UNSET
    model: str | Unset = UNSET
    model_identity: str | Unset = UNSET
    embedding_options: DeciderConfigEmbeddingOptions | Unset = UNSET
    url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    max_rows: int | Unset = 10000
    max_input_tokens: int | Unset = 1000000
    max_image_input_tokens: int | Unset = UNSET
    rate_limit: RateLimitConfig | Unset = UNSET
    batch_size: int | Unset = 32

    def to_dict(self) -> dict[str, Any]:
        provider: str | Unset = UNSET
        if not isinstance(self.provider, Unset):
            provider = self.provider.value

        decision_method: str | Unset = UNSET
        if not isinstance(self.decision_method, Unset):
            decision_method = self.decision_method.value

        generator: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        instructions = self.instructions

        output: str | Unset = UNSET
        if not isinstance(self.output, Unset):
            output = self.output.value

        probabilities: str | Unset = UNSET
        if not isinstance(self.probabilities, Unset):
            probabilities = self.probabilities.value

        batch: dict[str, Any] | Unset = UNSET
        if not isinstance(self.batch, Unset):
            batch = self.batch.to_dict()

        model = self.model

        model_identity = self.model_identity

        embedding_options: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding_options, Unset):
            embedding_options = self.embedding_options.to_dict()

        url = self.url

        api_key = self.api_key

        max_rows = self.max_rows

        max_input_tokens = self.max_input_tokens

        max_image_input_tokens = self.max_image_input_tokens

        rate_limit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.rate_limit, Unset):
            rate_limit = self.rate_limit.to_dict()

        batch_size = self.batch_size

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if provider is not UNSET:
            field_dict["provider"] = provider
        if decision_method is not UNSET:
            field_dict["decision_method"] = decision_method
        if generator is not UNSET:
            field_dict["generator"] = generator
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if output is not UNSET:
            field_dict["output"] = output
        if probabilities is not UNSET:
            field_dict["probabilities"] = probabilities
        if batch is not UNSET:
            field_dict["batch"] = batch
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
        if max_image_input_tokens is not UNSET:
            field_dict["max_image_input_tokens"] = max_image_input_tokens
        if rate_limit is not UNSET:
            field_dict["rate_limit"] = rate_limit
        if batch_size is not UNSET:
            field_dict["batch_size"] = batch_size

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.constrained_generation_batch import ConstrainedGenerationBatch
        from ..models.decider_config_embedding_options import DeciderConfigEmbeddingOptions
        from ..models.generator_config import GeneratorConfig
        from ..models.rate_limit_config import RateLimitConfig

        d = dict(src_dict)
        _provider = d.pop("provider", UNSET)
        provider: DeciderConfigProvider | Unset
        if isinstance(_provider, Unset):
            provider = UNSET
        else:
            provider = DeciderConfigProvider(_provider)

        _decision_method = d.pop("decision_method", UNSET)
        decision_method: DeciderConfigDecisionMethod | Unset
        if isinstance(_decision_method, Unset):
            decision_method = UNSET
        else:
            decision_method = DeciderConfigDecisionMethod(_decision_method)

        _generator = d.pop("generator", UNSET)
        generator: GeneratorConfig | Unset
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        instructions = d.pop("instructions", UNSET)

        _output = d.pop("output", UNSET)
        output: ConstrainedOutput | Unset
        if isinstance(_output, Unset):
            output = UNSET
        else:
            output = ConstrainedOutput(_output)

        _probabilities = d.pop("probabilities", UNSET)
        probabilities: ConstrainedProbabilities | Unset
        if isinstance(_probabilities, Unset):
            probabilities = UNSET
        else:
            probabilities = ConstrainedProbabilities(_probabilities)

        _batch = d.pop("batch", UNSET)
        batch: ConstrainedGenerationBatch | Unset
        if isinstance(_batch, Unset):
            batch = UNSET
        else:
            batch = ConstrainedGenerationBatch.from_dict(_batch)

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

        max_image_input_tokens = d.pop("max_image_input_tokens", UNSET)

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
            generator=generator,
            instructions=instructions,
            output=output,
            probabilities=probabilities,
            batch=batch,
            model=model,
            model_identity=model_identity,
            embedding_options=embedding_options,
            url=url,
            api_key=api_key,
            max_rows=max_rows,
            max_input_tokens=max_input_tokens,
            max_image_input_tokens=max_image_input_tokens,
            rate_limit=rate_limit,
            batch_size=batch_size,
        )

        return decider_config
