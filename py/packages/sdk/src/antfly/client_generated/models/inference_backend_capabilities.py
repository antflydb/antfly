from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceBackendCapabilities")


@_attrs_define
class InferenceBackendCapabilities:
    """
    Attributes:
        native (bool | Unset): Whether the native CPU backend is built into this runtime Example: True.
        onnx (bool | Unset): Whether the ONNX Runtime backend is built into this runtime
        mlx (bool | Unset): Whether the MLX backend is built into this runtime Example: True.
        wasm (bool | Unset): Whether the WASM backend is built into this runtime
    """

    native: bool | Unset = UNSET
    onnx: bool | Unset = UNSET
    mlx: bool | Unset = UNSET
    wasm: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        native = self.native

        onnx = self.onnx

        mlx = self.mlx

        wasm = self.wasm

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if native is not UNSET:
            field_dict["native"] = native
        if onnx is not UNSET:
            field_dict["onnx"] = onnx
        if mlx is not UNSET:
            field_dict["mlx"] = mlx
        if wasm is not UNSET:
            field_dict["wasm"] = wasm

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        native = d.pop("native", UNSET)

        onnx = d.pop("onnx", UNSET)

        mlx = d.pop("mlx", UNSET)

        wasm = d.pop("wasm", UNSET)

        inference_backend_capabilities = cls(
            native=native,
            onnx=onnx,
            mlx=mlx,
            wasm=wasm,
        )

        inference_backend_capabilities.additional_properties = d
        return inference_backend_capabilities

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
