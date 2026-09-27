from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.text_analysis_component import TextAnalysisComponent


T = TypeVar("T", bound="TextAnalysisConfigAnalyzers")


@_attrs_define
class TextAnalysisConfigAnalyzers:
    """Named analyzers of type `custom`. `config.tokenizer` names a built-in or declared tokenizer; `config.char_filters`
    and `config.token_filters` list built-in or declared component names in application order. Configuration-free
    filters (`lowercase`, `stop_words`, `stemmer`, `camel_case`, `unique`, `reverse`, `elision`, `apostrophe`, `suffix`)
    can be listed by name without declaring them.

    """

    additional_properties: dict[str, TextAnalysisComponent] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.text_analysis_component import TextAnalysisComponent

        d = dict(src_dict)
        text_analysis_config_analyzers = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = TextAnalysisComponent.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        text_analysis_config_analyzers.additional_properties = additional_properties
        return text_analysis_config_analyzers

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> TextAnalysisComponent:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: TextAnalysisComponent) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
