from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.row_unique_selector_values import RowUniqueSelectorValues


T = TypeVar("T", bound="RowUniqueSelector")


@_attrs_define
class RowUniqueSelector:
    """Structured unique-key selector. The server encodes `values` with the
    same relational tuple encoder used by storage, routes to the durable
    unique-owner range, and reads the owner row to resolve the physical row
    identity. This is a point lookup, not a query scan.

        Attributes:
            name (str): Unique constraint name.
            values (RowUniqueSelectorValues): Values for the declared unique-constraint columns.
    """

    name: str
    values: RowUniqueSelectorValues
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        values = self.values.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "values": values,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.row_unique_selector_values import RowUniqueSelectorValues

        d = dict(src_dict)
        name = d.pop("name")

        values = RowUniqueSelectorValues.from_dict(d.pop("values"))

        row_unique_selector = cls(
            name=name,
            values=values,
        )

        row_unique_selector.additional_properties = d
        return row_unique_selector

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
