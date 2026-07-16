from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_model_backend import InferenceModelBackend
from ..models.inference_model_format import InferenceModelFormat
from ..models.inference_model_kind import InferenceModelKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceModelRef")


@_attrs_define
class InferenceModelRef:
    """Model reference used by startup preload and model-loading configuration.

    Attributes:
        kind (InferenceModelKind): Model registry kind.
        name (str): Model name to resolve within the registry for the selected kind, usually in
            `<owner>/<repo>` format. Generation requests can address a preloaded artifact
            explicitly as `<owner>/<repo>:<format>:<quantization>`.
             Example: antflydb/gemma-e2b.
        backend (InferenceModelBackend | Unset): Optional backend preference for model loading or request execution.
            `auto` keeps the node default behavior.
            `pjrt` selects the PJRT backend and requires `pjrt_plugin_path` unless
            the standard `PJRT_PLUGIN_PATH` environment variable is set.
            `webgpu` selects the Wasm/WebGPU backend in Wasm builds; pair it with
            `mode: "compiled"` on generation requests to request WebGPU graph partition execution.
        format_ (InferenceModelFormat | Unset): Optional artifact family to select when a model directory contains
            multiple loadable formats.
        quantization (str | Unset): Optional exact quantization selector within the chosen artifact family. Matching is
            case-insensitive and treats `-` and `_` equivalently (for example, `q4_k` matches
            `Q4_K`). The configured model must contain exactly one matching artifact variant.
            Quantization is not valid with the composite `hybrid` format.
             Example: q4_k.
    """

    kind: InferenceModelKind
    name: str
    backend: InferenceModelBackend | Unset = UNSET
    format_: InferenceModelFormat | Unset = UNSET
    quantization: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        name = self.name

        backend: str | Unset = UNSET
        if not isinstance(self.backend, Unset):
            backend = self.backend.value

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        quantization = self.quantization

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
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

        quantization = d.pop("quantization", UNSET)

        inference_model_ref = cls(
            kind=kind,
            name=name,
            backend=backend,
            format_=format_,
            quantization=quantization,
        )

        inference_model_ref.additional_properties = d
        return inference_model_ref

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
