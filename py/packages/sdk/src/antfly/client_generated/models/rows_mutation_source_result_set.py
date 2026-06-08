from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_mutation_source_result_set_returning_item import RowsMutationSourceResultSetReturningItem


T = TypeVar("T", bound="RowsMutationSourceResultSet")


@_attrs_define
class RowsMutationSourceResultSet:
    """
    Attributes:
        matched (int | Unset): Number of source rows that matched before lock/limit selection.
        staged (int | Unset): Number of rows staged into the claimed transaction.
        returning (list[RowsMutationSourceResultSetReturningItem] | Unset): Optional returning rows from the staged
            mutation.
    """

    matched: int | Unset = UNSET
    staged: int | Unset = UNSET
    returning: list[RowsMutationSourceResultSetReturningItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        matched = self.matched

        staged = self.staged

        returning: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.returning, Unset):
            returning = []
            for returning_item_data in self.returning:
                returning_item = returning_item_data.to_dict()
                returning.append(returning_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if matched is not UNSET:
            field_dict["matched"] = matched
        if staged is not UNSET:
            field_dict["staged"] = staged
        if returning is not UNSET:
            field_dict["returning"] = returning

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_mutation_source_result_set_returning_item import RowsMutationSourceResultSetReturningItem

        d = dict(src_dict)
        matched = d.pop("matched", UNSET)

        staged = d.pop("staged", UNSET)

        _returning = d.pop("returning", UNSET)
        returning: list[RowsMutationSourceResultSetReturningItem] | Unset = UNSET
        if _returning is not UNSET:
            returning = []
            for returning_item_data in _returning:
                returning_item = RowsMutationSourceResultSetReturningItem.from_dict(returning_item_data)

                returning.append(returning_item)

        rows_mutation_source_result_set = cls(
            matched=matched,
            staged=staged,
            returning=returning,
        )

        rows_mutation_source_result_set.additional_properties = d
        return rows_mutation_source_result_set

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
