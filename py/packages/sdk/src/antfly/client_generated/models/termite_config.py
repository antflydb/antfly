from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_config_model_strategies import TermiteConfigModelStrategies
    from ..models.termite_content_security_config import TermiteContentSecurityConfig
    from ..models.termite_credentials import TermiteCredentials
    from ..models.termiteschemas_config import TermiteschemasConfig


T = TypeVar("T", bound="TermiteConfig")


@_attrs_define
class TermiteConfig:
    """
    Attributes:
        api_url (str): URL of the Termite embedding/chunking service Example: http://localhost:8080.
        models_dir (str | Unset): Base directory containing model subdirectories. Termite auto-discovers models from:
            - `{models_dir}/embedders/` - Embedding models (ONNX)
            - `{models_dir}/chunkers/` - Chunking models (ONNX)
            - `{models_dir}/rerankers/` - Reranking models (ONNX)
            - `{models_dir}/recognizers/` - Recognition models (ONNX)
            - `{models_dir}/rewriters/` - Seq2Seq rewriter models (ONNX)

            Defaults to ~/.termite/models (set via viper). If not set, only built-in fixed chunking is available.
             Example: ~/.termite/models.
        content_security (TermiteContentSecurityConfig | Unset):
        s3_credentials (TermiteCredentials | Unset):
        keep_alive (str | Unset): How long to keep models loaded in memory after last use (Ollama-compatible).
            Models are automatically unloaded after this duration of inactivity.
            Use Go duration format: "5m" (5 minutes), "1h" (1 hour), "0" (eager loading).
            Defaults to "5m" (lazy loading) like Ollama. Set to "0" to explicitly enable eager loading
            where all models are loaded at startup and never unloaded.
             Default: '5m'. Example: 5m.
        max_loaded_models (int | Unset): Maximum total models loaded across all registry types (embedders, rerankers,
            generators, chunkers, etc.). When the limit is reached, the least-recently-used
            idle model from any registry is evicted to make room. Set to 0 for unlimited (default).
             Default: 0. Example: 3.
        pool_size (int | Unset): Number of concurrent inference pipelines per model. Each pipeline loads
            a copy of the model, so higher values use more memory but allow more
            concurrent requests. Note: pool_size multiplies per-model memory
            independently of max_loaded_models.
             Default: 1. Example: 1.
        backend_priority (list[str] | Unset): Backend priority order for model loading with optional device specifiers.
            Format: `backend` or `backend:device` where device defaults to `auto`.

            Termite tries entries in order and uses the first available backend+device
            combination that supports the model.

            **Backends** (depend on build tags):
            - `go` - Pure Go inference (always available, CPU only, slowest)
            - `onnx` - ONNX Runtime (requires -tags="onnx,ORT", fastest)
            - `xla` - GoMLX XLA (requires -tags="xla,XLA", TPU/CUDA/CPU)

            **Devices**:
            - `auto` - Auto-detect best available (default)
            - `cuda` - NVIDIA CUDA GPU
            - `tpu` - Google TPU (used by XLA)
            - `cpu` - Force CPU only

            **Examples**:
            - `["onnx", "xla", "go"]` - Try backends with auto device detection
            - `["onnx:cuda", "xla:tpu", "onnx:cpu", "go"]` - Prefer GPU, fall back to CPU
            - `          default:
            - onnx
            - xla
            - go
             Example: ['onnx:cuda', 'xla:tpu', 'onnx:cpu', 'xla:cpu', 'go'].
        max_concurrent_requests (int | Unset): Maximum number of concurrent inference requests allowed.
            Additional requests will be queued up to max_queue_size.
            Set to 0 for unlimited (default).
             Default: 0. Example: 4.
        max_queue_size (int | Unset): Maximum number of requests to queue when max_concurrent_requests is reached.
            When the queue is full, new requests receive 503 Service Unavailable with Retry-After header.
            Set to 0 for unlimited queue (default). Only effective when max_concurrent_requests > 0.
             Default: 0. Example: 100.
        request_timeout (str | Unset): Maximum time to wait for a request to complete, including queue wait time.
            Use Go duration format: "30s", "1m", "0" (no timeout, default).
            Requests exceeding this timeout receive 504 Gateway Timeout.
             Default: '0'. Example: 30s.
        preload (list[str] | Unset): List of model names to preload at startup (Ollama-compatible).
            These models are loaded immediately when Termite starts, avoiding first-request latency.
            Model names should match those in models_dir/embedders/ (e.g., "BAAI/bge-small-en-v1.5").
            Only effective when keep_alive is non-zero (lazy loading mode).
             Example: ['BAAI/bge-small-en-v1.5', 'openai/clip-vit-base-patch32'].
        max_memory_mb (int | Unset): Maximum memory (in MB) to use for loaded models.
            When this limit is approached, least recently used models are unloaded.
            Set to 0 for unlimited (default). This is an advisory limit - actual memory
            usage depends on model sizes and may temporarily exceed this value.
            Works alongside max_loaded_models for fine-grained control.
             Default: 0. Example: 4096.
        model_strategies (TermiteConfigModelStrategies | Unset): Per-model loading strategy overrides. Maps model names
            to their loading strategy.
            Models not in this map use the default strategy based on keep_alive:
            - If keep_alive>0 (default "5m"): lazy loading (load on demand, unload after idle)
            - If keep_alive="0": eager loading (load at startup, never unload)

            When a model has strategy "eager" in this map:
            - It is loaded at startup (as part of preload)
            - It is never unloaded, even when keep_alive>0 (pinned in memory)

            This allows mixing eager and lazy models in the same pool.
             Example: {'BAAI/bge-small-en-v1.5': 'eager', 'mirth/chonky-mmbert-small-multilingual-1': 'lazy'}.
        allow_downloads (bool | Unset): Whether the dashboard should show model download commands.
            Defaults to true for standalone/swarm mode. Set to false in managed
            deployments (e.g., Kubernetes operator) where models are managed externally.
             Default: True.
        log (TermiteschemasConfig | Unset): Logging configuration for Termite services
    """

    api_url: str
    models_dir: str | Unset = UNSET
    content_security: TermiteContentSecurityConfig | Unset = UNSET
    s3_credentials: TermiteCredentials | Unset = UNSET
    keep_alive: str | Unset = "5m"
    max_loaded_models: int | Unset = 0
    pool_size: int | Unset = 1
    backend_priority: list[str] | Unset = UNSET
    max_concurrent_requests: int | Unset = 0
    max_queue_size: int | Unset = 0
    request_timeout: str | Unset = "0"
    preload: list[str] | Unset = UNSET
    max_memory_mb: int | Unset = 0
    model_strategies: TermiteConfigModelStrategies | Unset = UNSET
    allow_downloads: bool | Unset = True
    log: TermiteschemasConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        api_url = self.api_url

        models_dir = self.models_dir

        content_security: dict[str, Any] | Unset = UNSET
        if not isinstance(self.content_security, Unset):
            content_security = self.content_security.to_dict()

        s3_credentials: dict[str, Any] | Unset = UNSET
        if not isinstance(self.s3_credentials, Unset):
            s3_credentials = self.s3_credentials.to_dict()

        keep_alive = self.keep_alive

        max_loaded_models = self.max_loaded_models

        pool_size = self.pool_size

        backend_priority: list[str] | Unset = UNSET
        if not isinstance(self.backend_priority, Unset):
            backend_priority = self.backend_priority

        max_concurrent_requests = self.max_concurrent_requests

        max_queue_size = self.max_queue_size

        request_timeout = self.request_timeout

        preload: list[str] | Unset = UNSET
        if not isinstance(self.preload, Unset):
            preload = self.preload

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
        if models_dir is not UNSET:
            field_dict["models_dir"] = models_dir
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
        from ..models.termite_config_model_strategies import TermiteConfigModelStrategies
        from ..models.termite_content_security_config import TermiteContentSecurityConfig
        from ..models.termite_credentials import TermiteCredentials
        from ..models.termiteschemas_config import TermiteschemasConfig

        d = dict(src_dict)
        api_url = d.pop("api_url")

        models_dir = d.pop("models_dir", UNSET)

        _content_security = d.pop("content_security", UNSET)
        content_security: TermiteContentSecurityConfig | Unset
        if isinstance(_content_security, Unset):
            content_security = UNSET
        else:
            content_security = TermiteContentSecurityConfig.from_dict(_content_security)

        _s3_credentials = d.pop("s3_credentials", UNSET)
        s3_credentials: TermiteCredentials | Unset
        if isinstance(_s3_credentials, Unset):
            s3_credentials = UNSET
        else:
            s3_credentials = TermiteCredentials.from_dict(_s3_credentials)

        keep_alive = d.pop("keep_alive", UNSET)

        max_loaded_models = d.pop("max_loaded_models", UNSET)

        pool_size = d.pop("pool_size", UNSET)

        backend_priority = cast(list[str], d.pop("backend_priority", UNSET))

        max_concurrent_requests = d.pop("max_concurrent_requests", UNSET)

        max_queue_size = d.pop("max_queue_size", UNSET)

        request_timeout = d.pop("request_timeout", UNSET)

        preload = cast(list[str], d.pop("preload", UNSET))

        max_memory_mb = d.pop("max_memory_mb", UNSET)

        _model_strategies = d.pop("model_strategies", UNSET)
        model_strategies: TermiteConfigModelStrategies | Unset
        if isinstance(_model_strategies, Unset):
            model_strategies = UNSET
        else:
            model_strategies = TermiteConfigModelStrategies.from_dict(_model_strategies)

        allow_downloads = d.pop("allow_downloads", UNSET)

        _log = d.pop("log", UNSET)
        log: TermiteschemasConfig | Unset
        if isinstance(_log, Unset):
            log = UNSET
        else:
            log = TermiteschemasConfig.from_dict(_log)

        termite_config = cls(
            api_url=api_url,
            models_dir=models_dir,
            content_security=content_security,
            s3_credentials=s3_credentials,
            keep_alive=keep_alive,
            max_loaded_models=max_loaded_models,
            pool_size=pool_size,
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

        termite_config.additional_properties = d
        return termite_config

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
