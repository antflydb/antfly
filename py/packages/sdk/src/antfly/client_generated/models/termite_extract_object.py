from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_extract_object_object import TermiteExtractObjectObject

if TYPE_CHECKING:
    from ..models.termite_extract_object_results import TermiteExtractObjectResults


T = TypeVar("T", bound="TermiteExtractObject")


@_attrs_define
class TermiteExtractObject:
    """
    Attributes:
        object_ (TermiteExtractObjectObject):
        index (int): Original input index.
        results (TermiteExtractObjectResults): Extraction result for this input. Maps structure names to arrays of
            extracted instances.
            Each instance maps field names to ExtractFieldValue (for ::str fields)
            or arrays of ExtractFieldValue (for ::list fields).
    """

    object_: TermiteExtractObjectObject
    index: int
    results: TermiteExtractObjectResults
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        index = self.index

        results = self.results.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "index": index,
                "results": results,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_extract_object_results import TermiteExtractObjectResults

        d = dict(src_dict)
        object_ = TermiteExtractObjectObject(d.pop("object"))

        index = d.pop("index")

        results = TermiteExtractObjectResults.from_dict(d.pop("results"))

        termite_extract_object = cls(
            object_=object_,
            index=index,
            results=results,
        )

        termite_extract_object.additional_properties = d
        return termite_extract_object

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
