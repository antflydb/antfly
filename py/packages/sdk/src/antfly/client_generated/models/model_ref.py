from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.a4b_residency_mode import A4BResidencyMode
from ..models.model_backend import ModelBackend
from ..models.model_format import ModelFormat
from ..models.model_kind import ModelKind
from ..models.model_quantization import ModelQuantization
from ..types import UNSET, Unset

T = TypeVar("T", bound="ModelRef")


@_attrs_define
class ModelRef:
    """Model reference used by startup preload and model-loading configuration.
    When a preload selects an explicit backend, that warmed session also
    becomes the model's default in-process session for later `auto`
    requests; callers do not need to repeat the backend on every request.

        Attributes:
            kind (ModelKind): Model registry kind.
            name (str): Model name to resolve within the registry for the selected kind, usually in `<owner>/<repo>` format.
                Example: antflydb/gemma-e2b.
            pin (bool | Unset): Keep a successfully preloaded model pinned against eviction. Set false to warm the ordinary
                evictable cache before serving; idle and model-count policy still apply. Default: True.
            backend (ModelBackend | Unset): Optional backend preference for model loading or request execution.
                `auto` keeps the node default behavior.
                `xla` selects the PJRT/XLA backend and may require a PJRT plugin path via
                `ANTFLY_INFERENCE_XLA_PLUGIN`, `ANTFLY_INFERENCE_PJRT_PLUGIN`,
                `PJRT_PLUGIN_PATH`, or `PJRT_PLUGIN`.
                `webgpu` selects the Wasm/WebGPU backend in Wasm builds.
            format_ (ModelFormat | Unset): Optional artifact format preference for loading a model.
            quantization (ModelQuantization | Unset): Optional quantization preference for loading a model.
            residency_mode (A4BResidencyMode | Unset): Load-time residency policy for the qualified Gemma 4 26B-A4B Q4_0
                Metal or CUDA runtime. On Metal, `auto` chooses full residency only when
                the complete expert set and safety reserves fit the configured budget;
                otherwise it uses bounded streaming. On the qualified SM89 CUDA lane,
                `auto` resolves to resident mode and fails closed unless its envelope fits.
            memory_budget_mb (int | Unset): Per-model A4B memory envelope in MiB. Zero selects the backend default:
                the conservative 2048 MiB streamed floor on Metal, or the qualified
                16384 MiB resident envelope on CUDA. Explicit values below 2048 fail;
                CUDA also rejects any envelope too small for full residency. Other
                model geometries reject this field.
                 Default: 0.
    """

    kind: ModelKind
    name: str
    pin: bool | Unset = True
    backend: ModelBackend | Unset = UNSET
    format_: ModelFormat | Unset = UNSET
    quantization: ModelQuantization | Unset = UNSET
    residency_mode: A4BResidencyMode | Unset = UNSET
    memory_budget_mb: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        name = self.name

        pin = self.pin

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

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "name": name,
            }
        )
        if pin is not UNSET:
            field_dict["pin"] = pin
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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = ModelKind(d.pop("kind"))

        name = d.pop("name")

        pin = d.pop("pin", UNSET)

        _backend = d.pop("backend", UNSET)
        backend: ModelBackend | Unset
        if isinstance(_backend, Unset):
            backend = UNSET
        else:
            backend = ModelBackend(_backend)

        _format_ = d.pop("format", UNSET)
        format_: ModelFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = ModelFormat(_format_)

        _quantization = d.pop("quantization", UNSET)
        quantization: ModelQuantization | Unset
        if isinstance(_quantization, Unset):
            quantization = UNSET
        else:
            quantization = ModelQuantization(_quantization)

        _residency_mode = d.pop("residency_mode", UNSET)
        residency_mode: A4BResidencyMode | Unset
        if isinstance(_residency_mode, Unset):
            residency_mode = UNSET
        else:
            residency_mode = A4BResidencyMode(_residency_mode)

        memory_budget_mb = d.pop("memory_budget_mb", UNSET)

        model_ref = cls(
            kind=kind,
            name=name,
            pin=pin,
            backend=backend,
            format_=format_,
            quantization=quantization,
            residency_mode=residency_mode,
            memory_budget_mb=memory_budget_mb,
        )

        return model_ref
