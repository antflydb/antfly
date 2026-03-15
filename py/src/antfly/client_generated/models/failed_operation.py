from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.failed_operation_operation import FailedOperationOperation
from ..types import UNSET, Unset

T = TypeVar("T", bound="FailedOperation")


@_attrs_define
class FailedOperation:
    """
    Attributes:
        id (Union[Unset, str]):
        operation (Union[Unset, FailedOperationOperation]):
        error (Union[Unset, str]):
    """

    id: Union[Unset, str] = UNSET
    operation: Union[Unset, FailedOperationOperation] = UNSET
    error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        operation: Union[Unset, str] = UNSET
        if not isinstance(self.operation, Unset):
            operation = self.operation.value

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if id is not UNSET:
            field_dict["id"] = id
        if operation is not UNSET:
            field_dict["operation"] = operation
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id", UNSET)

        _operation = d.pop("operation", UNSET)
        operation: Union[Unset, FailedOperationOperation]
        if isinstance(_operation, Unset):
            operation = UNSET
        else:
            operation = FailedOperationOperation(_operation)

        error = d.pop("error", UNSET)

        failed_operation = cls(
            id=id,
            operation=operation,
            error=error,
        )

        failed_operation.additional_properties = d
        return failed_operation

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
