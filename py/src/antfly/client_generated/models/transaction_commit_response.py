from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.transaction_commit_response_status import TransactionCommitResponseStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transaction_commit_response_conflict import TransactionCommitResponseConflict
    from ..models.transaction_commit_response_tables import TransactionCommitResponseTables


T = TypeVar("T", bound="TransactionCommitResponse")


@_attrs_define
class TransactionCommitResponse:
    """Result of an OCC transaction commit attempt.

    Attributes:
        status (TransactionCommitResponseStatus): Whether the transaction was committed or aborted due to a conflict
        conflict (Union[Unset, TransactionCommitResponseConflict]): Details about the conflict that caused an abort
            (only present when status is "aborted")
        tables (Union[Unset, TransactionCommitResponseTables]): Per-table batch results (only present when status is
            "committed")
    """

    status: TransactionCommitResponseStatus
    conflict: Union[Unset, "TransactionCommitResponseConflict"] = UNSET
    tables: Union[Unset, "TransactionCommitResponseTables"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        conflict: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.conflict, Unset):
            conflict = self.conflict.to_dict()

        tables: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.tables, Unset):
            tables = self.tables.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "status": status,
            }
        )
        if conflict is not UNSET:
            field_dict["conflict"] = conflict
        if tables is not UNSET:
            field_dict["tables"] = tables

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transaction_commit_response_conflict import TransactionCommitResponseConflict
        from ..models.transaction_commit_response_tables import TransactionCommitResponseTables

        d = dict(src_dict)
        status = TransactionCommitResponseStatus(d.pop("status"))

        _conflict = d.pop("conflict", UNSET)
        conflict: Union[Unset, TransactionCommitResponseConflict]
        if isinstance(_conflict, Unset):
            conflict = UNSET
        else:
            conflict = TransactionCommitResponseConflict.from_dict(_conflict)

        _tables = d.pop("tables", UNSET)
        tables: Union[Unset, TransactionCommitResponseTables]
        if isinstance(_tables, Unset):
            tables = UNSET
        else:
            tables = TransactionCommitResponseTables.from_dict(_tables)

        transaction_commit_response = cls(
            status=status,
            conflict=conflict,
            tables=tables,
        )

        transaction_commit_response.additional_properties = d
        return transaction_commit_response

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
