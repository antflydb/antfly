from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.rows_unique_predicate import RowsUniquePredicate


T = TypeVar("T", bound="RowsUniquePredicateGroup")


@_attrs_define
class RowsUniquePredicateGroup:
    """Conjunction of partial-unique predicate atoms.

    Attributes:
        all_ (list[RowsUniquePredicate]):
    """

    all_: list[RowsUniquePredicate]

    def to_dict(self) -> dict[str, Any]:
        all_ = []
        for all_item_data in self.all_:
            all_item = all_item_data.to_dict()
            all_.append(all_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "all": all_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_unique_predicate import RowsUniquePredicate

        d = dict(src_dict)
        all_ = []
        _all_ = d.pop("all")
        for all_item_data in _all_:
            all_item = RowsUniquePredicate.from_dict(all_item_data)

            all_.append(all_item)

        rows_unique_predicate_group = cls(
            all_=all_,
        )

        return rows_unique_predicate_group
