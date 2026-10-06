from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.text_analysis_component_config import TextAnalysisComponentConfig


T = TypeVar("T", bound="TextAnalysisComponent")


@_attrs_define
class TextAnalysisComponent:
    """One named analysis component: its type and type-specific configuration.

    Attributes:
        type_ (str):
        config (TextAnalysisComponentConfig | Unset):
    """

    type_: str
    config: TextAnalysisComponentConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_

        config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.config, Unset):
            config = self.config.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
            }
        )
        if config is not UNSET:
            field_dict["config"] = config

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.text_analysis_component_config import TextAnalysisComponentConfig

        d = dict(src_dict)
        type_ = d.pop("type")

        _config = d.pop("config", UNSET)
        config: TextAnalysisComponentConfig | Unset
        if isinstance(_config, Unset):
            config = UNSET
        else:
            config = TextAnalysisComponentConfig.from_dict(_config)

        text_analysis_component = cls(
            type_=type_,
            config=config,
        )

        text_analysis_component.additional_properties = d
        return text_analysis_component

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
