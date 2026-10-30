from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.config import Config
    from ..models.content_security_config import ContentSecurityConfig
    from ..models.credentials import Credentials
    from ..models.kernel_jit_config import KernelJitConfig
    from ..models.model_ref import ModelRef
    from ..models.prompt_cache_config import PromptCacheConfig
    from ..models.runtime_config_generation_batching import RuntimeConfigGenerationBatching
    from ..models.runtime_config_model_strategies import RuntimeConfigModelStrategies


T = TypeVar("T", bound="RuntimeConfig")


@_attrs_define
class RuntimeConfig:
    """
    Attributes:
        http_max_body_mb (int | Unset): Encoded HTTP inference body ceiling in MiB, independent of decoded-media
            resource policy. Default: 10.
        api_url (str | Unset): URL of an out-of-process Antfly inference service that the Antfly
            server should call for embedding, chunking, reranking, and
            generation. Omit it to use the in-process inference runtime
            (the default in standalone mode). `antfly inference run` ignores
            this field; it configures the client side only.
             Example: http://localhost:8080.
        api_key (str | Unset): Client-only API key used when calling an authenticated shared Antfly inference API. Both
            standalone inference executables accept and ignore this field when serving models.
        models_dir (str | Unset): Base directory containing model subdirectories. Antfly inference auto-discovers models
            from:
            - `{models_dir}/{owner}/{name}/` - Models fetched with `antfly inference pull owner/name`
            - `{models_dir}/embedders/` - Embedding models
            - `{models_dir}/rerankers/` - Reranking models
            - `{models_dir}/extractors/` - Entity, relation, and structured extraction models
            - `{models_dir}/rewriters/` - Seq2Seq rewriter models

            Defaults to `$ANTFLY_INFERENCE_MODELS_DIR` when set, otherwise `~/.antfly/inference/models`
            (`./models` when `$HOME` is unset). The `--models-dir` flag overrides both. The built-in `fixed`
            chunker needs no models.
             Example: ~/.antfly/inference/models.
        ml_dir (str | Unset): Base directory containing Traditional ML predictor subdirectories. The `/ml/v1/*`
            API auto-discovers predictors from `{ml_dir}/{name}/tabular_model.json`.

            Defaults to ~/.antfly/inference/ml.
             Example: ~/.antfly/inference/ml.
        content_security (ContentSecurityConfig | Unset): Low-level content download policy. File downloads always
            require an
            explicit nonempty `allowed_paths` list. In the generic scraper, omitted
            `allowed_hosts` for HTTP(S) and `allowed_paths` for S3 do not restrict
            those sources; explicit empty lists deny them. Consumers may merge this object over a
            stricter baseline before downloading. Antfly inference does so: omitted
            or empty inference policies, and omitted inference allowlists, deny
            HTTP(S), file, and S3 content until explicit allowlists are configured.
            Do not assume omission has identical policy semantics across consumers.
        s3_credentials (Credentials | Unset):
        generation_batching (RuntimeConfigGenerationBatching | Unset): Experimental execution batching; mode on
            explicitly enables qualified CUDA scheduling. Resource and request ceilings still apply.
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
        prompt_cache (PromptCacheConfig | Unset): Native generator prompt KV cache configuration.
        kernel_jit (KernelJitConfig | Unset): Runtime Metal and CUDA kernel JIT configuration. Live compilation and
            GPU qualification run only for models in the configured startup
            `preload` list, before serving begins. A cold dynamic model load never
            benchmarks against an in-use GPU: `on` and `shadow` retain bundled
            kernels, while `required` rejects the load. When runtime JIT is enabled,
            startup also materializes every vision, audio, and projection session
            declared by each preloaded model without running media inference.
            Preload every model that must use runtime-JIT kernels; `required`
            rejects an empty preload list.
        backend_priority (list[str] | Unset): Legacy compatibility field. The current Zig inference runtime selects a
            backend from model metadata, explicit preload settings, and compiled capabilities;
            configuring this list has no effect.
        max_queue_size (int | Unset): Legacy Go-runtime queue setting. The current Zig runtime does not retain
            excess inference requests in memory and ignores this field.
        request_timeout (str | Unset): Legacy Go-runtime queue/request timeout. The current Zig runtime ignores
            this field; its HTTP listener applies a separate fixed transport timeout.
        preload (list[ModelRef] | Unset): Models to preload and warm at startup. Generators run a tiny generation
            request so native/Metal weights, KV setup, and kernels use the same
            budgeted path as request-time generation. Other model kinds use the
            best available warm path for that kind. When runtime kernel JIT is
            enabled, every vision, audio, and projection session declared by a
            preloaded model is also loaded without running media inference.
            Runtime kernel JIT `required` mode rejects an empty preload list.
             Example: [{'kind': 'generator', 'name': 'antflydb/gemma-e2b', 'backend': 'metal', 'format': 'gguf',
            'quantization': 'q4_k'}].
        max_memory_mb (int | Unset): Legacy compatibility field. The current Zig runtime uses explicit host,
            backend, combined, KV, and scratch budgets instead and ignores this field.
        model_strategies (RuntimeConfigModelStrategies | Unset): Per-model loading strategy overrides. Maps model names
            to their loading strategy.
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
        log (Config | Unset): Logging configuration for Antfly inference services
    """

    http_max_body_mb: int | Unset = 10
    api_url: str | Unset = UNSET
    api_key: str | Unset = UNSET
    models_dir: str | Unset = UNSET
    ml_dir: str | Unset = UNSET
    content_security: ContentSecurityConfig | Unset = UNSET
    s3_credentials: Credentials | Unset = UNSET
    generation_batching: RuntimeConfigGenerationBatching | Unset = UNSET
    keep_alive: str | Unset = "5m"
    max_loaded_models: int | Unset = 10
    pool_size: int | Unset = UNSET
    prompt_cache: PromptCacheConfig | Unset = UNSET
    kernel_jit: KernelJitConfig | Unset = UNSET
    backend_priority: list[str] | Unset = UNSET
    max_queue_size: int | Unset = UNSET
    request_timeout: str | Unset = UNSET
    preload: list[ModelRef] | Unset = UNSET
    max_memory_mb: int | Unset = UNSET
    model_strategies: RuntimeConfigModelStrategies | Unset = UNSET
    allow_downloads: bool | Unset = True
    log: Config | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        http_max_body_mb = self.http_max_body_mb

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

        generation_batching: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generation_batching, Unset):
            generation_batching = self.generation_batching.to_dict()

        keep_alive = self.keep_alive

        max_loaded_models = self.max_loaded_models

        pool_size = self.pool_size

        prompt_cache: dict[str, Any] | Unset = UNSET
        if not isinstance(self.prompt_cache, Unset):
            prompt_cache = self.prompt_cache.to_dict()

        kernel_jit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.kernel_jit, Unset):
            kernel_jit = self.kernel_jit.to_dict()

        backend_priority: list[str] | Unset = UNSET
        if not isinstance(self.backend_priority, Unset):
            backend_priority = self.backend_priority

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
        field_dict.update({})
        if http_max_body_mb is not UNSET:
            field_dict["http_max_body_mb"] = http_max_body_mb
        if api_url is not UNSET:
            field_dict["api_url"] = api_url
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
        if generation_batching is not UNSET:
            field_dict["generation_batching"] = generation_batching
        if keep_alive is not UNSET:
            field_dict["keep_alive"] = keep_alive
        if max_loaded_models is not UNSET:
            field_dict["max_loaded_models"] = max_loaded_models
        if pool_size is not UNSET:
            field_dict["pool_size"] = pool_size
        if prompt_cache is not UNSET:
            field_dict["prompt_cache"] = prompt_cache
        if kernel_jit is not UNSET:
            field_dict["kernel_jit"] = kernel_jit
        if backend_priority is not UNSET:
            field_dict["backend_priority"] = backend_priority
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
        from ..models.config import Config
        from ..models.content_security_config import ContentSecurityConfig
        from ..models.credentials import Credentials
        from ..models.kernel_jit_config import KernelJitConfig
        from ..models.model_ref import ModelRef
        from ..models.prompt_cache_config import PromptCacheConfig
        from ..models.runtime_config_generation_batching import RuntimeConfigGenerationBatching
        from ..models.runtime_config_model_strategies import RuntimeConfigModelStrategies

        d = dict(src_dict)
        http_max_body_mb = d.pop("http_max_body_mb", UNSET)

        api_url = d.pop("api_url", UNSET)

        api_key = d.pop("api_key", UNSET)

        models_dir = d.pop("models_dir", UNSET)

        ml_dir = d.pop("ml_dir", UNSET)

        _content_security = d.pop("content_security", UNSET)
        content_security: ContentSecurityConfig | Unset
        if isinstance(_content_security, Unset):
            content_security = UNSET
        else:
            content_security = ContentSecurityConfig.from_dict(_content_security)

        _s3_credentials = d.pop("s3_credentials", UNSET)
        s3_credentials: Credentials | Unset
        if isinstance(_s3_credentials, Unset):
            s3_credentials = UNSET
        else:
            s3_credentials = Credentials.from_dict(_s3_credentials)

        _generation_batching = d.pop("generation_batching", UNSET)
        generation_batching: RuntimeConfigGenerationBatching | Unset
        if isinstance(_generation_batching, Unset):
            generation_batching = UNSET
        else:
            generation_batching = RuntimeConfigGenerationBatching.from_dict(_generation_batching)

        keep_alive = d.pop("keep_alive", UNSET)

        max_loaded_models = d.pop("max_loaded_models", UNSET)

        pool_size = d.pop("pool_size", UNSET)

        _prompt_cache = d.pop("prompt_cache", UNSET)
        prompt_cache: PromptCacheConfig | Unset
        if isinstance(_prompt_cache, Unset):
            prompt_cache = UNSET
        else:
            prompt_cache = PromptCacheConfig.from_dict(_prompt_cache)

        _kernel_jit = d.pop("kernel_jit", UNSET)
        kernel_jit: KernelJitConfig | Unset
        if isinstance(_kernel_jit, Unset):
            kernel_jit = UNSET
        else:
            kernel_jit = KernelJitConfig.from_dict(_kernel_jit)

        backend_priority = cast(list[str], d.pop("backend_priority", UNSET))

        max_queue_size = d.pop("max_queue_size", UNSET)

        request_timeout = d.pop("request_timeout", UNSET)

        _preload = d.pop("preload", UNSET)
        preload: list[ModelRef] | Unset = UNSET
        if _preload is not UNSET:
            preload = []
            for preload_item_data in _preload:
                preload_item = ModelRef.from_dict(preload_item_data)

                preload.append(preload_item)

        max_memory_mb = d.pop("max_memory_mb", UNSET)

        _model_strategies = d.pop("model_strategies", UNSET)
        model_strategies: RuntimeConfigModelStrategies | Unset
        if isinstance(_model_strategies, Unset):
            model_strategies = UNSET
        else:
            model_strategies = RuntimeConfigModelStrategies.from_dict(_model_strategies)

        allow_downloads = d.pop("allow_downloads", UNSET)

        _log = d.pop("log", UNSET)
        log: Config | Unset
        if isinstance(_log, Unset):
            log = UNSET
        else:
            log = Config.from_dict(_log)

        runtime_config = cls(
            http_max_body_mb=http_max_body_mb,
            api_url=api_url,
            api_key=api_key,
            models_dir=models_dir,
            ml_dir=ml_dir,
            content_security=content_security,
            s3_credentials=s3_credentials,
            generation_batching=generation_batching,
            keep_alive=keep_alive,
            max_loaded_models=max_loaded_models,
            pool_size=pool_size,
            prompt_cache=prompt_cache,
            kernel_jit=kernel_jit,
            backend_priority=backend_priority,
            max_queue_size=max_queue_size,
            request_timeout=request_timeout,
            preload=preload,
            max_memory_mb=max_memory_mb,
            model_strategies=model_strategies,
            allow_downloads=allow_downloads,
            log=log,
        )

        runtime_config.additional_properties = d
        return runtime_config

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
