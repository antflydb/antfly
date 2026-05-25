from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_read_response_object import TermiteReadResponseObject

if TYPE_CHECKING:
    from ..models.termite_generate_usage import TermiteGenerateUsage
    from ..models.termite_read_object import TermiteReadObject


T = TypeVar("T", bound="TermiteReadResponse")


@_attrs_define
class TermiteReadResponse:
    """
    Attributes:
        object_ (TermiteReadResponseObject): Object type, always "list"
        data (list[TermiteReadObject]): Read result objects, one per input image.
        model (str): Name of model used for reading
        usage (TermiteGenerateUsage):
    """

    object_: TermiteReadResponseObject
    data: list[TermiteReadObject]
    model: str
    usage: TermiteGenerateUsage
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        model = self.model

        usage = self.usage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "model": model,
                "usage": usage,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_generate_usage import TermiteGenerateUsage
        from ..models.termite_read_object import TermiteReadObject

        d = dict(src_dict)
        object_ = TermiteReadResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = TermiteReadObject.from_dict(data_item_data)

            data.append(data_item)

        model = d.pop("model")

        usage = TermiteGenerateUsage.from_dict(d.pop("usage"))

        termite_read_response = cls(
            object_=object_,
            data=data,
            model=model,
            usage=usage,
        )

        termite_read_response.additional_properties = d
        return termite_read_response

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
