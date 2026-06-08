from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.row_primary_selector import RowPrimarySelector
    from ..models.row_unique_selector import RowUniqueSelector


T = TypeVar("T", bound="RowSelector")


@_attrs_define
class RowSelector:
    """Structured row selector. `primary` addresses declared primary-key
    tables directly. `unique` addresses a declared unique constraint through
    durable unique-owner rows.

        Attributes:
            primary (RowPrimarySelector | Unset): Structured primary-key identity. Keys are the declared
                `primary_key.columns`; values are JSON scalar values coerced with the
                table's relational column types.
                 Example: {'tenant_id': 't1', 'order_id': 'o9'}.
            unique (RowUniqueSelector | Unset): Structured unique-key selector. The server encodes `values` with the
                same relational tuple encoder used by storage, routes to the durable
                unique-owner range, and reads the owner row to resolve the physical row
                identity. This is a point lookup, not a query scan.
    """

    primary: RowPrimarySelector | Unset = UNSET
    unique: RowUniqueSelector | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        primary: dict[str, Any] | Unset = UNSET
        if not isinstance(self.primary, Unset):
            primary = self.primary.to_dict()

        unique: dict[str, Any] | Unset = UNSET
        if not isinstance(self.unique, Unset):
            unique = self.unique.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if primary is not UNSET:
            field_dict["primary"] = primary
        if unique is not UNSET:
            field_dict["unique"] = unique

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.row_primary_selector import RowPrimarySelector
        from ..models.row_unique_selector import RowUniqueSelector

        d = dict(src_dict)
        _primary = d.pop("primary", UNSET)
        primary: RowPrimarySelector | Unset
        if isinstance(_primary, Unset):
            primary = UNSET
        else:
            primary = RowPrimarySelector.from_dict(_primary)

        _unique = d.pop("unique", UNSET)
        unique: RowUniqueSelector | Unset
        if isinstance(_unique, Unset):
            unique = UNSET
        else:
            unique = RowUniqueSelector.from_dict(_unique)

        row_selector = cls(
            primary=primary,
            unique=unique,
        )

        row_selector.additional_properties = d
        return row_selector

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
