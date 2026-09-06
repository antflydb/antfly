from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_a4b_load_strategy import InferenceA4BLoadStrategy
from ..models.inference_a4b_prepared_pack_mode import InferenceA4BPreparedPackMode
from ..models.inference_a4b_residency_mode import InferenceA4BResidencyMode
from ..models.inference_model_backend import InferenceModelBackend
from ..models.inference_model_format import InferenceModelFormat
from ..models.inference_model_kind import InferenceModelKind
from ..models.inference_model_quantization import InferenceModelQuantization
from ..models.inference_warm_model_startup_strategy import InferenceWarmModelStartupStrategy
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelRef")


@_attrs_define
class InferenceModelRef:
    """Model reference used by startup preload and model-loading configuration.

    Attributes:
        kind (InferenceModelKind): Model registry kind.
        name (str): Model name to resolve within the registry for the selected kind, usually in `<owner>/<repo>` format.
            Example: antflydb/gemma-e2b.
        backend (InferenceModelBackend | Unset): Optional backend preference for model loading or request execution.
            `auto` keeps the node default behavior.
            `xla` selects the PJRT/XLA backend and may require a PJRT plugin path via
            `ANTFLY_INFERENCE_XLA_PLUGIN`, `ANTFLY_INFERENCE_PJRT_PLUGIN`,
            `PJRT_PLUGIN_PATH`, or `PJRT_PLUGIN`.
            `webgpu` selects the Wasm/WebGPU backend in Wasm builds; pair it with
            `mode: "compiled"` on generation requests to request WebGPU graph partition execution.
        format_ (InferenceModelFormat | Unset): Optional artifact format preference for loading a model.
        quantization (InferenceModelQuantization | Unset): Optional quantization preference for loading a model.
        residency_mode (InferenceA4BResidencyMode | Unset): Load-time residency policy for the qualified Gemma 4 26B-A4B
            Q4_0 Metal or CUDA runtime. On qualified SM89 CUDA, auto resolves to resident and fails closed unless its
            envelope fits.
        memory_budget_mb (int | Unset): Per-model A4B memory envelope in MiB. Zero selects the backend default (2048 MiB
            streamed on Metal or 16384 MiB resident on qualified CUDA); CUDA rejects any envelope too small for full
            residency. Other model geometries reject this field. Default: 0.
        load_strategy (InferenceA4BLoadStrategy | Unset): Loader implementation for qualified Gemma 4 26B-A4B Q4_0
            loads. Auto selects the production default, pipeline requires the bounded pinned-host pipeline, and legacy
            selects the single-threaded loader.
        load_workers (int | Unset): Bounded loader worker count for qualified A4B loads. Zero selects the runtime
            default. Default: 0.
        load_staging_mb (int | Unset): Aggregate pinned-host staging budget in MiB. Zero selects the runtime default;
            explicit values must be between 64 and 1024. Default: 0.
        prepared_pack (InferenceA4BPreparedPackMode | Unset): Prepared-pack policy for qualified A4B CUDA loads.
            Required fails closed unless a valid pack is installed.
        drop_host_cache_after_load (bool | Unset): Drop clean GGUF pages from the host page cache after a successful A4B
            load. Default: False.
        startup_strategy (InferenceWarmModelStartupStrategy | Unset): Eager loads and publishes a reusable session.
            Prefetch only reads A4B CUDA artifact pages into the host page cache and does not publish a session.
    """

    kind: InferenceModelKind
    name: str
    backend: InferenceModelBackend | Unset = UNSET
    format_: InferenceModelFormat | Unset = UNSET
    quantization: InferenceModelQuantization | Unset = UNSET
    residency_mode: InferenceA4BResidencyMode | Unset = UNSET
    memory_budget_mb: int | Unset = 0
    load_strategy: InferenceA4BLoadStrategy | Unset = UNSET
    load_workers: int | Unset = 0
    load_staging_mb: int | Unset = 0
    prepared_pack: InferenceA4BPreparedPackMode | Unset = UNSET
    drop_host_cache_after_load: bool | Unset = False
    startup_strategy: InferenceWarmModelStartupStrategy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        name = self.name

        backend: str | Unset = UNSET
        if not isinstance(self.backend, Unset):
            backend = self.backend.value

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        quantization: str | Unset = UNSET
        if not isinstance(self.quantization, Unset):
            quantization = self.quantization.value

        residency_mode: str | Unset = UNSET
        if not isinstance(self.residency_mode, Unset):
            residency_mode = self.residency_mode.value

        memory_budget_mb = self.memory_budget_mb

        load_strategy: str | Unset = UNSET
        if not isinstance(self.load_strategy, Unset):
            load_strategy = self.load_strategy.value

        load_workers = self.load_workers

        load_staging_mb = self.load_staging_mb

        prepared_pack: str | Unset = UNSET
        if not isinstance(self.prepared_pack, Unset):
            prepared_pack = self.prepared_pack.value

        drop_host_cache_after_load = self.drop_host_cache_after_load

        startup_strategy: str | Unset = UNSET
        if not isinstance(self.startup_strategy, Unset):
            startup_strategy = self.startup_strategy.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "name": name,
            }
        )
        if backend is not UNSET:
            field_dict["backend"] = backend
        if format_ is not UNSET:
            field_dict["format"] = format_
        if quantization is not UNSET:
            field_dict["quantization"] = quantization
        if residency_mode is not UNSET:
            field_dict["residency_mode"] = residency_mode
        if memory_budget_mb is not UNSET:
            field_dict["memory_budget_mb"] = memory_budget_mb
        if load_strategy is not UNSET:
            field_dict["load_strategy"] = load_strategy
        if load_workers is not UNSET:
            field_dict["load_workers"] = load_workers
        if load_staging_mb is not UNSET:
            field_dict["load_staging_mb"] = load_staging_mb
        if prepared_pack is not UNSET:
            field_dict["prepared_pack"] = prepared_pack
        if drop_host_cache_after_load is not UNSET:
            field_dict["drop_host_cache_after_load"] = drop_host_cache_after_load
        if startup_strategy is not UNSET:
            field_dict["startup_strategy"] = startup_strategy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = InferenceModelKind(d.pop("kind"))

        name = d.pop("name")

        _backend = d.pop("backend", UNSET)
        backend: InferenceModelBackend | Unset
        if isinstance(_backend, Unset):
            backend = UNSET
        else:
            backend = InferenceModelBackend(_backend)

        _format_ = d.pop("format", UNSET)
        format_: InferenceModelFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = InferenceModelFormat(_format_)

        _quantization = d.pop("quantization", UNSET)
        quantization: InferenceModelQuantization | Unset
        if isinstance(_quantization, Unset):
            quantization = UNSET
        else:
            quantization = InferenceModelQuantization(_quantization)

        _residency_mode = d.pop("residency_mode", UNSET)
        residency_mode: InferenceA4BResidencyMode | Unset
        if isinstance(_residency_mode, Unset):
            residency_mode = UNSET
        else:
            residency_mode = InferenceA4BResidencyMode(_residency_mode)

        memory_budget_mb = d.pop("memory_budget_mb", UNSET)

        _load_strategy = d.pop("load_strategy", UNSET)
        load_strategy: InferenceA4BLoadStrategy | Unset
        if isinstance(_load_strategy, Unset):
            load_strategy = UNSET
        else:
            load_strategy = InferenceA4BLoadStrategy(_load_strategy)

        load_workers = d.pop("load_workers", UNSET)

        load_staging_mb = d.pop("load_staging_mb", UNSET)

        _prepared_pack = d.pop("prepared_pack", UNSET)
        prepared_pack: InferenceA4BPreparedPackMode | Unset
        if isinstance(_prepared_pack, Unset):
            prepared_pack = UNSET
        else:
            prepared_pack = InferenceA4BPreparedPackMode(_prepared_pack)

        drop_host_cache_after_load = d.pop("drop_host_cache_after_load", UNSET)

        _startup_strategy = d.pop("startup_strategy", UNSET)
        startup_strategy: InferenceWarmModelStartupStrategy | Unset
        if isinstance(_startup_strategy, Unset):
            startup_strategy = UNSET
        else:
            startup_strategy = InferenceWarmModelStartupStrategy(_startup_strategy)

        inference_model_ref = cls(
            kind=kind,
            name=name,
            backend=backend,
            format_=format_,
            quantization=quantization,
            residency_mode=residency_mode,
            memory_budget_mb=memory_budget_mb,
            load_strategy=load_strategy,
            load_workers=load_workers,
            load_staging_mb=load_staging_mb,
            prepared_pack=prepared_pack,
            drop_host_cache_after_load=drop_host_cache_after_load,
            startup_strategy=startup_strategy,
        )

        return inference_model_ref
