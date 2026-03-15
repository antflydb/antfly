from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ForeignColumn")


@_attrs_define
class ForeignColumn:
    """
    Attributes:
        name (str): Column name in the foreign table. Example: email.
        type_ (str): Column data type. Used for filter validation and type coercion.
            Common types: text, integer, bigint, float, boolean, timestamp, uuid, jsonb.
             Example: text.
        nullable (Union[Unset, bool]): Whether the column allows NULL values. Default: False.
    """

    name: str
    type_: str
    nullable: Union[Unset, bool] = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        type_ = self.type_

        nullable = self.nullable

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "type": type_,
            }
        )
        if nullable is not UNSET:
            field_dict["nullable"] = nullable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        type_ = d.pop("type")

        nullable = d.pop("nullable", UNSET)

        foreign_column = cls(
            name=name,
            type_=type_,
            nullable=nullable,
        )

        foreign_column.additional_properties = d
        return foreign_column

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
