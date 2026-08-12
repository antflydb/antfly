from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_config_model_strategies import InferenceConfigModelStrategies
    from ..models.inference_content_security_config import InferenceContentSecurityConfig
    from ..models.inference_credentials import InferenceCredentials
    from ..models.inference_model_ref import InferenceModelRef
    from ..models.inference_prompt_cache_config import InferencePromptCacheConfig
    from ..models.inferenceschemas_config import InferenceschemasConfig


T = TypeVar("T", bound="InferenceConfig")


@_attrs_define
class InferenceConfig:
    """
    Attributes:
        api_url (str): URL of the Antfly inference embedding/chunking service Example: http://localhost:8080.
        api_key (str | Unset): API key used when calling an authenticated shared Antfly inference API.
        models_dir (str | Unset): Base directory containing model subdirectories. Antfly inference auto-discovers models
            from:
            - `{models_dir}/embedders/` - Embedding models (ONNX)
            - `{models_dir}/chunkers/` - Chunking models (ONNX)
            - `{models_dir}/rerankers/` - Reranking models (ONNX)
            - `{models_dir}/extractors/` - Entity, relation, and structured extraction models
            - `{models_dir}/rewriters/` - Seq2Seq rewriter models (ONNX)

            Defaults to ~/.antfly/inference/models (set via viper). If not set, only built-in fixed chunking is available.
             Example: ~/.antfly/inference/models.
        ml_dir (str | Unset): Base directory containing Traditional ML predictor subdirectories. The `/ml/v1/*`
            API auto-discovers predictors from `{ml_dir}/{name}/tabular_model.json`.

            Defaults to ~/.antfly/inference/ml.
             Example: ~/.antfly/inference/ml.
        content_security (InferenceContentSecurityConfig | Unset): Inference merges configured fields over a fail-closed
            baseline. HTTP(S), file, and S3 content require explicit allowlists; data URIs remain allowed within the
            configured size budget.
        s3_credentials (InferenceCredentials | Unset):
        keep_alive (str | Unset): How long to keep models loaded in memory after last use (Ollama-compatible).
            Models are automatically unloaded after this duration of inactivity.
            Use Go duration format: "5m" (5 minutes), "1h" (1 hour), or "0".
            Defaults to "5m". Set to "0" to disable idle-time eviction; models can
            still be evicted under resource pressure or to enforce max_loaded_models.
             Default: '5m'. Example: 5m.
        max_loaded_models (int | Unset): Maximum total models loaded across all registry types (embedders, rerankers,
            generators, chunkers, etc.). When the limit is reached, the least-recently-used
            idle model from any registry is evicted to make room. Set to 0 for unlimited.
            Defaults to 10.
             Default: 10. Example: 3.
        pool_size (int | Unset): Legacy compatibility field. The current Zig inference runtime does not
            create per-model pipeline pools from this setting; configuring it has no effect.
        prompt_cache (InferencePromptCacheConfig | Unset): Native generator prompt KV cache configuration.
        backend_priority (list[str] | Unset): Legacy compatibility field. The current Zig inference runtime selects a
            backend from model metadata, explicit preload settings, and compiled capabilities;
            configuring this list has no effect.
        max_concurrent_requests (int | Unset): Maximum concurrent weighted inference admission units in the Zig runtime.
            Request body size, generation workload, and image byte/count reservations
            can consume more than one unit. Read and image-extraction admission reserves the
            effective downloaded-byte ceiling at 16 MiB per unit and at least one unit per
            two images. A positive capacity also clamps each such request's downloaded-image
            ceiling to 16 MiB times this value. Set to 0 disables both admission accounting
            and that capacity-derived clamp.
            When a positive limit is exhausted, new requests are rejected immediately
            with 503 Service Unavailable and Retry-After: 1; they are not retained in
            an in-process queue. Set to 0 only as an operational escape hatch for
            trusted testing environments; unlimited admission is not recommended for
            production native generation. Use a positive production limit. The default is 32.
             Default: 32. Example: 32.
        max_queue_size (int | Unset): Legacy Go-runtime queue setting. The current Zig runtime does not retain
            excess inference requests in memory and ignores this field.
        request_timeout (str | Unset): Legacy Go-runtime queue/request timeout. The current Zig runtime ignores
            this field; its HTTP listener applies a separate fixed transport timeout.
        preload (list[InferenceModelRef] | Unset): Models to preload and warm at startup. Generators run a tiny
            generation
            request so native/Metal weights, KV setup, and kernels use the same
            budgeted path as request-time generation. Other model kinds use the
            best available warm path for that kind.
             Example: [{'kind': 'generator', 'name': 'antflydb/gemma-e2b', 'backend': 'metal', 'format': 'gguf',
            'quantization': 'q4_k'}].
        max_memory_mb (int | Unset): Legacy compatibility field. The current Zig runtime uses explicit host,
            backend, combined, KV, and scratch budgets instead and ignores this field.
        model_strategies (InferenceConfigModelStrategies | Unset): Per-model loading strategy overrides. Maps model
            names to their loading strategy.
            Models not in this map load on demand. keep_alive controls their idle
            eviction; setting it to "0" disables idle eviction but does not preload or pin them.

            When a model has strategy "eager" in this map:
            - It is loaded at startup through the same startup warmup path
            - It is never unloaded, even when keep_alive>0 (pinned in memory)

            This allows mixing eager and lazy models in the same pool.
             Example: {'BAAI/bge-small-en-v1.5': 'eager', 'mirth/chonky-mmbert-small-multilingual-1': 'lazy'}.
        allow_downloads (bool | Unset): Legacy compatibility field controlling whether dashboards show model
            download commands. It defaults to true for standalone deployments;
            managed deployments historically set it to false. Download-command
            availability is a build-time setting in the current Zig runtime, so
            configuring this field has no effect.
             Default: True.
        log (InferenceschemasConfig | Unset): Legacy inference-local logging configuration. The current unified Zig
            runtime ignores it; configure the top-level `log` object instead.
    """

    api_url: str
    api_key: str | Unset = UNSET
    models_dir: str | Unset = UNSET
    ml_dir: str | Unset = UNSET
    content_security: InferenceContentSecurityConfig | Unset = UNSET
    s3_credentials: InferenceCredentials | Unset = UNSET
    keep_alive: str | Unset = "5m"
    max_loaded_models: int | Unset = 10
    pool_size: int | Unset = UNSET
    prompt_cache: InferencePromptCacheConfig | Unset = UNSET
    backend_priority: list[str] | Unset = UNSET
    max_concurrent_requests: int | Unset = 32
    max_queue_size: int | Unset = UNSET
    request_timeout: str | Unset = UNSET
    preload: list[InferenceModelRef] | Unset = UNSET
    max_memory_mb: int | Unset = UNSET
    model_strategies: InferenceConfigModelStrategies | Unset = UNSET
    allow_downloads: bool | Unset = True
    log: InferenceschemasConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        api_url = self.api_url

        api_key = self.api_key

        models_dir = self.models_dir

        ml_dir = self.ml_dir

        content_security: dict[str, Any] | Unset = UNSET
        if not isinstance(self.content_security, Unset):
            content_security = self.content_security.to_dict()

        s3_credentials: dict[str, Any] | Unset = UNSET
        if not isinstance(self.s3_credentials, Unset):
            s3_credentials = self.s3_credentials.to_dict()

        keep_alive = self.keep_alive

        max_loaded_models = self.max_loaded_models

        pool_size = self.pool_size

        prompt_cache: dict[str, Any] | Unset = UNSET
        if not isinstance(self.prompt_cache, Unset):
            prompt_cache = self.prompt_cache.to_dict()

        backend_priority: list[str] | Unset = UNSET
        if not isinstance(self.backend_priority, Unset):
            backend_priority = self.backend_priority

        max_concurrent_requests = self.max_concurrent_requests

        max_queue_size = self.max_queue_size

        request_timeout = self.request_timeout

        preload: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.preload, Unset):
            preload = []
            for preload_item_data in self.preload:
                preload_item = preload_item_data.to_dict()
                preload.append(preload_item)

        max_memory_mb = self.max_memory_mb

        model_strategies: dict[str, Any] | Unset = UNSET
        if not isinstance(self.model_strategies, Unset):
            model_strategies = self.model_strategies.to_dict()

        allow_downloads = self.allow_downloads

        log: dict[str, Any] | Unset = UNSET
        if not isinstance(self.log, Unset):
            log = self.log.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "api_url": api_url,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if models_dir is not UNSET:
            field_dict["models_dir"] = models_dir
        if ml_dir is not UNSET:
            field_dict["ml_dir"] = ml_dir
        if content_security is not UNSET:
            field_dict["content_security"] = content_security
        if s3_credentials is not UNSET:
            field_dict["s3_credentials"] = s3_credentials
        if keep_alive is not UNSET:
            field_dict["keep_alive"] = keep_alive
        if max_loaded_models is not UNSET:
            field_dict["max_loaded_models"] = max_loaded_models
        if pool_size is not UNSET:
            field_dict["pool_size"] = pool_size
        if prompt_cache is not UNSET:
            field_dict["prompt_cache"] = prompt_cache
        if backend_priority is not UNSET:
            field_dict["backend_priority"] = backend_priority
        if max_concurrent_requests is not UNSET:
            field_dict["max_concurrent_requests"] = max_concurrent_requests
        if max_queue_size is not UNSET:
            field_dict["max_queue_size"] = max_queue_size
        if request_timeout is not UNSET:
            field_dict["request_timeout"] = request_timeout
        if preload is not UNSET:
            field_dict["preload"] = preload
        if max_memory_mb is not UNSET:
            field_dict["max_memory_mb"] = max_memory_mb
        if model_strategies is not UNSET:
            field_dict["model_strategies"] = model_strategies
        if allow_downloads is not UNSET:
            field_dict["allow_downloads"] = allow_downloads
        if log is not UNSET:
            field_dict["log"] = log

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_config_model_strategies import InferenceConfigModelStrategies
        from ..models.inference_content_security_config import InferenceContentSecurityConfig
        from ..models.inference_credentials import InferenceCredentials
        from ..models.inference_model_ref import InferenceModelRef
        from ..models.inference_prompt_cache_config import InferencePromptCacheConfig
        from ..models.inferenceschemas_config import InferenceschemasConfig

        d = dict(src_dict)
        api_url = d.pop("api_url")

        api_key = d.pop("api_key", UNSET)

        models_dir = d.pop("models_dir", UNSET)

        ml_dir = d.pop("ml_dir", UNSET)

        _content_security = d.pop("content_security", UNSET)
        content_security: InferenceContentSecurityConfig | Unset
        if isinstance(_content_security, Unset):
            content_security = UNSET
        else:
            content_security = InferenceContentSecurityConfig.from_dict(_content_security)

        _s3_credentials = d.pop("s3_credentials", UNSET)
        s3_credentials: InferenceCredentials | Unset
        if isinstance(_s3_credentials, Unset):
            s3_credentials = UNSET
        else:
            s3_credentials = InferenceCredentials.from_dict(_s3_credentials)

        keep_alive = d.pop("keep_alive", UNSET)

        max_loaded_models = d.pop("max_loaded_models", UNSET)

        pool_size = d.pop("pool_size", UNSET)

        _prompt_cache = d.pop("prompt_cache", UNSET)
        prompt_cache: InferencePromptCacheConfig | Unset
        if isinstance(_prompt_cache, Unset):
            prompt_cache = UNSET
        else:
            prompt_cache = InferencePromptCacheConfig.from_dict(_prompt_cache)

        backend_priority = cast(list[str], d.pop("backend_priority", UNSET))

        max_concurrent_requests = d.pop("max_concurrent_requests", UNSET)

        max_queue_size = d.pop("max_queue_size", UNSET)

        request_timeout = d.pop("request_timeout", UNSET)

        _preload = d.pop("preload", UNSET)
        preload: list[InferenceModelRef] | Unset = UNSET
        if _preload is not UNSET:
            preload = []
            for preload_item_data in _preload:
                preload_item = InferenceModelRef.from_dict(preload_item_data)

                preload.append(preload_item)

        max_memory_mb = d.pop("max_memory_mb", UNSET)

        _model_strategies = d.pop("model_strategies", UNSET)
        model_strategies: InferenceConfigModelStrategies | Unset
        if isinstance(_model_strategies, Unset):
            model_strategies = UNSET
        else:
            model_strategies = InferenceConfigModelStrategies.from_dict(_model_strategies)

        allow_downloads = d.pop("allow_downloads", UNSET)

        _log = d.pop("log", UNSET)
        log: InferenceschemasConfig | Unset
        if isinstance(_log, Unset):
            log = UNSET
        else:
            log = InferenceschemasConfig.from_dict(_log)

        inference_config = cls(
            api_url=api_url,
            api_key=api_key,
            models_dir=models_dir,
            ml_dir=ml_dir,
            content_security=content_security,
            s3_credentials=s3_credentials,
            keep_alive=keep_alive,
            max_loaded_models=max_loaded_models,
            pool_size=pool_size,
            prompt_cache=prompt_cache,
            backend_priority=backend_priority,
            max_concurrent_requests=max_concurrent_requests,
            max_queue_size=max_queue_size,
            request_timeout=request_timeout,
            preload=preload,
            max_memory_mb=max_memory_mb,
            model_strategies=model_strategies,
            allow_downloads=allow_downloads,
            log=log,
        )

        inference_config.additional_properties = d
        return inference_config

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
