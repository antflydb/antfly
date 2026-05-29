from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_response_object import ExtractionResponseObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extraction_object import ExtractionObject
    from ..models.extraction_response_usage import ExtractionResponseUsage


T = TypeVar("T", bound="ExtractionResponse")


@_attrs_define
class ExtractionResponse:
    """
    Attributes:
        object_ (ExtractionResponseObject):
        model (str):
        data (list[ExtractionObject]):
        usage (ExtractionResponseUsage | Unset):
    """

    object_: ExtractionResponseObject
    model: str
    data: list[ExtractionObject]
    usage: ExtractionResponseUsage | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        model = self.model

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        usage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.usage, Unset):
            usage = self.usage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "model": model,
                "data": data,
            }
        )
        if usage is not UNSET:
            field_dict["usage"] = usage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extraction_object import ExtractionObject
        from ..models.extraction_response_usage import ExtractionResponseUsage

        d = dict(src_dict)
        object_ = ExtractionResponseObject(d.pop("object"))

        model = d.pop("model")

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = ExtractionObject.from_dict(data_item_data)

            data.append(data_item)

        _usage = d.pop("usage", UNSET)
        usage: ExtractionResponseUsage | Unset
        if isinstance(_usage, Unset):
            usage = UNSET
        else:
            usage = ExtractionResponseUsage.from_dict(_usage)

        extraction_response = cls(
            object_=object_,
            model=model,
            data=data,
            usage=usage,
        )

        extraction_response.additional_properties = d
        return extraction_response

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
