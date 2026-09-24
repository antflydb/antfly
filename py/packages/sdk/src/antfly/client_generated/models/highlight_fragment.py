from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.highlight_span import HighlightSpan


T = TypeVar("T", bound="HighlightFragment")


@_attrs_define
class HighlightFragment:
    """
    Attributes:
        text (str): The fragment of the stored field value.
        offset (int): Byte offset of the fragment within the field value.
        spans (list[HighlightSpan]):
        item (int | Unset): Array index of the value when the field is an array of strings.
    """

    text: str
    offset: int
    spans: list[HighlightSpan]
    item: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        text = self.text

        offset = self.offset

        spans = []
        for spans_item_data in self.spans:
            spans_item = spans_item_data.to_dict()
            spans.append(spans_item)

        item = self.item

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "text": text,
                "offset": offset,
                "spans": spans,
            }
        )
        if item is not UNSET:
            field_dict["item"] = item

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.highlight_span import HighlightSpan

        d = dict(src_dict)
        text = d.pop("text")

        offset = d.pop("offset")

        spans = []
        _spans = d.pop("spans")
        for spans_item_data in _spans:
            spans_item = HighlightSpan.from_dict(spans_item_data)

            spans.append(spans_item)

        item = d.pop("item", UNSET)

        highlight_fragment = cls(
            text=text,
            offset=offset,
            spans=spans,
            item=item,
        )

        highlight_fragment.additional_properties = d
        return highlight_fragment

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
