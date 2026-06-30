from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.row_unique_selector_values import RowUniqueSelectorValues


T = TypeVar("T", bound="RowUniqueSelector")


@_attrs_define
class RowUniqueSelector:
    """Structured unique-key selector. The server encodes `values` with the
    same relational tuple encoder used by storage, routes to the durable
    unique-owner range, and reads the owner row to resolve the physical row
    identity. This is a point lookup, not a query scan. The selector object
    is exact: only `name` and `values` are accepted.

        Attributes:
            name (str): Unique constraint name.
            values (RowUniqueSelectorValues): Values for the declared unique-constraint columns.
    """

    name: str
    values: RowUniqueSelectorValues

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        values = self.values.to_dict()

        field_dict: dict[str, Any] = {}

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

        return row_unique_selector
