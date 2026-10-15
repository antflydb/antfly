from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_model_info_qualified_limits import InferenceModelInfoQualifiedLimits


T = TypeVar("T", bound="InferenceModelInfo")


@_attrs_define
class InferenceModelInfo:
    """Information about a model including its capabilities

    Attributes:
        model_revision (str | Unset): Immutable resolved embedding generation, returned for scoped model/task discovery
            without loading weights.
        qualified_limits (InferenceModelInfoQualifiedLimits | Unset): Pull-time qualified request profiles for this
            exact artifact,
            including backend, supported features and min/max geometry.
            Runtime admission revalidates artifact identity and resource limits.
            Omitted for artifacts without a qualification receipt.
        capabilities (list[str] | Unset): List of capabilities this model supports (omitted when empty). For rerankers,
            `late_interaction` or `colbert` selects native MaxSim token scoring.
        inputs (list[str] | Unset): List of input modalities this model accepts, such as `text`, `image`, or `audio`
    """

    model_revision: str | Unset = UNSET
    qualified_limits: InferenceModelInfoQualifiedLimits | Unset = UNSET
    capabilities: list[str] | Unset = UNSET
    inputs: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model_revision = self.model_revision

        qualified_limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.qualified_limits, Unset):
            qualified_limits = self.qualified_limits.to_dict()

        capabilities: list[str] | Unset = UNSET
        if not isinstance(self.capabilities, Unset):
            capabilities = self.capabilities

        inputs: list[str] | Unset = UNSET
        if not isinstance(self.inputs, Unset):
            inputs = self.inputs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model_revision is not UNSET:
            field_dict["model_revision"] = model_revision
        if qualified_limits is not UNSET:
            field_dict["qualified_limits"] = qualified_limits
        if capabilities is not UNSET:
            field_dict["capabilities"] = capabilities
        if inputs is not UNSET:
            field_dict["inputs"] = inputs

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_model_info_qualified_limits import InferenceModelInfoQualifiedLimits

        d = dict(src_dict)
        model_revision = d.pop("model_revision", UNSET)

        _qualified_limits = d.pop("qualified_limits", UNSET)
        qualified_limits: InferenceModelInfoQualifiedLimits | Unset
        if isinstance(_qualified_limits, Unset):
            qualified_limits = UNSET
        else:
            qualified_limits = InferenceModelInfoQualifiedLimits.from_dict(_qualified_limits)

        capabilities = cast(list[str], d.pop("capabilities", UNSET))

        inputs = cast(list[str], d.pop("inputs", UNSET))

        inference_model_info = cls(
            model_revision=model_revision,
            qualified_limits=qualified_limits,
            capabilities=capabilities,
            inputs=inputs,
        )

        inference_model_info.additional_properties = d
        return inference_model_info

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
