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
            - `{models_dir}/recognizers/` - Recognition models (ONNX)
            - `{models_dir}/rewriters/` - Seq2Seq rewriter models (ONNX)

            Defaults to ~/.antfly/inference/models (set via viper). If not set, only built-in fixed chunking is available.
             Example: ~/.antfly/inference/models.
        ml_dir (str | Unset): Base directory containing Traditional ML predictor subdirectories. The `/ml/v1/*`
            API auto-discovers predictors from `{ml_dir}/{name}/tabular_model.json`.

            Defaults to ~/.antfly/inference/ml.
             Example: ~/.antfly/inference/ml.
        content_security (InferenceContentSecurityConfig | Unset):
        s3_credentials (InferenceCredentials | Unset):
        keep_alive (str | Unset): Idle-eviction policy for loaded models (Ollama-compatible). Runtimes that
            support idle eviction may unload a model after this duration of inactivity.
            Use Go duration format: "5m" (5 minutes), "1h" (1 hour), or "0".
            Defaults to "5m". Set to "0" to keep loaded models resident without idle
            eviction. For compatibility, some runtimes also eagerly load discovered
            models in this mode. Use `preload` when startup loading must be deterministic.
             Default: '5m'. Example: 5m.
        max_loaded_models (int | Unset): Maximum steady-state number of resident logical model instances. Manager-cached
            models and request-scoped specialized or composite pipelines share this budget.
            A composite pipeline counts as one logical model even when it owns multiple backend
            sessions. At capacity, one replacement may initialize while an idle cached model
            remains available; successful activation evicts that model. If no idle model is
            available, the request receives 503 Service Unavailable. Set to 0 for unlimited.
             Default: 10. Example: 3.
        pool_size (int | Unset): Number of reusable inference execution slots per model. Some runtimes realize
            each slot as a complete pipeline, increasing both possible concurrency and
            per-model memory; others pool lightweight backend providers, so memory and
            throughput effects are backend-dependent. Generation and backends with shared
            runtime state may remain serialized even when this is greater than one. Model
            residency is controlled separately by `max_loaded_models`.
             Default: 1. Example: 1.
        prompt_cache (InferencePromptCacheConfig | Unset): Native generator prompt KV cache configuration.
        backend_priority (list[str] | Unset): Backend priority order for model loading with optional device specifiers.
            Format: `backend` or `backend:device` where device defaults to `auto`.

            Antfly inference tries entries in order and uses the first available backend+device
            combination that supports the model.

            **Examples**:
            - `["native", "onnx", "xla"]` - Try backends with auto device detection
            - `["cuda", "onnx:cuda", "xla:tpu", "native"]` - Prefer GPU, fall back to CPU
             Example: ['cuda', 'onnx:cuda', 'xla:tpu', 'native'].
        max_concurrent_requests (int | Unset): Maximum weighted inference work admitted concurrently by the Zig runtime.
            Requests beyond the limit receive 503 Service Unavailable with Retry-After;
            they are not held in an in-process wait queue. Set to 0 for unlimited.
             Default: 32. Example: 4.
        max_queue_size (int | Unset): Compatibility field retained for older configs. The Zig runtime does not keep
            an in-process request wait queue and ignores this value; use
            max_concurrent_requests to configure immediate backpressure.
             Default: 0. Example: 100.
        request_timeout (str | Unset): Compatibility field retained for older configs. The Zig runtime does not
            currently apply a global request timeout from this value.
             Default: '0'. Example: 30s.
        preload (list[InferenceModelRef] | Unset): Models to preload and warm at startup. Generators run a tiny
            generation
            request so native/Metal weights, KV setup, and kernels use the same
            budgeted path as request-time generation. Other model kinds use the
            best available warm path for that kind. Specialized request-scoped pipelines
            are capacity-bounded but may still initialize on their first request.
             Example: [{'kind': 'generator', 'name': 'antflydb/gemma-e2b', 'backend': 'metal', 'format': 'gguf',
            'quantization': 'q4_k'}].
        max_memory_mb (int | Unset): Compatibility field for a future aggregate loaded-model memory limit. The Zig
            runtime does not currently enforce this value; use max_loaded_models for the
            logical-model admission and steady-state residency budget, not a byte limit. Set to
            0 when unused (default).
             Default: 0. Example: 4096.
        model_strategies (InferenceConfigModelStrategies | Unset): Per-model loading strategy overrides for runtimes
            that support them. Maps model
            names to their loading strategy. Models not in this map follow `keep_alive`:
            positive durations permit idle eviction, while "0" keeps a model resident after
            it is loaded. Some compatibility runtimes also eagerly load models for "0".

            When a model has strategy "eager" in this map:
            - It is loaded at startup through the same startup warmup path
            - It is never unloaded, even when keep_alive>0 (pinned in memory)

            Strategy support varies by runtime. Use `preload` for portable, deterministic
            startup loading.
             Example: {'BAAI/bge-small-en-v1.5': 'eager', 'mirth/chonky-mmbert-small-multilingual-1': 'lazy'}.
        allow_downloads (bool | Unset): Whether the dashboard should show model download commands.
            Defaults to true for standalone inference and Antfly standalone deployments. Set to false in managed
            deployments (e.g., Kubernetes operator) where models are managed externally.
             Default: True.
        log (InferenceschemasConfig | Unset): Logging configuration for inference services
    """

    api_url: str
    api_key: str | Unset = UNSET
    models_dir: str | Unset = UNSET
    ml_dir: str | Unset = UNSET
    content_security: InferenceContentSecurityConfig | Unset = UNSET
    s3_credentials: InferenceCredentials | Unset = UNSET
    keep_alive: str | Unset = "5m"
    max_loaded_models: int | Unset = 10
    pool_size: int | Unset = 1
    prompt_cache: InferencePromptCacheConfig | Unset = UNSET
    backend_priority: list[str] | Unset = UNSET
    max_concurrent_requests: int | Unset = 32
    max_queue_size: int | Unset = 0
    request_timeout: str | Unset = "0"
    preload: list[InferenceModelRef] | Unset = UNSET
    max_memory_mb: int | Unset = 0
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
