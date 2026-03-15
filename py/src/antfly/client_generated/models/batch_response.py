from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="BatchResponse")


@_attrs_define
class BatchResponse:
    """
    Attributes:
        inserted (Union[Unset, int]): Number of documents successfully inserted
        deleted (Union[Unset, int]): Number of documents successfully deleted
        transformed (Union[Unset, int]): Number of documents successfully transformed
    """

    inserted: Union[Unset, int] = UNSET
    deleted: Union[Unset, int] = UNSET
    transformed: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        inserted = self.inserted

        deleted = self.deleted

        transformed = self.transformed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if inserted is not UNSET:
            field_dict["inserted"] = inserted
        if deleted is not UNSET:
            field_dict["deleted"] = deleted
        if transformed is not UNSET:
            field_dict["transformed"] = transformed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        inserted = d.pop("inserted", UNSET)

        deleted = d.pop("deleted", UNSET)

        transformed = d.pop("transformed", UNSET)

        batch_response = cls(
            inserted=inserted,
            deleted=deleted,
            transformed=transformed,
        )

        batch_response.additional_properties = d
        return batch_response

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
