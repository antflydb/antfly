from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.transform_op import TransformOp


T = TypeVar("T", bound="Transform")


@_attrs_define
class Transform:
    """In-place document transformation using MongoDB-style operators. Transforms are applied atomically
    at the storage layer, eliminating read-modify-write races.

    **Important:** Transform results are NOT validated against the table schema. This improves performance
    but means it's possible to create invalid documents. Use with care and ensure your operations maintain
    schema compliance.

        Example:
            {'key': 'article:123', 'operations': [{'op': '$inc', 'path': '$.views', 'value': 1}, {'op': '$currentDate',
                'path': '$.lastViewed'}]}

        Attributes:
            key (str): Document key (must be a string, not an object like inserts)
            operations (list['TransformOp']): List of operations to apply in sequence
            upsert (Union[Unset, bool]): If true, create document if it doesn't exist (like MongoDB upsert) Default: False.
    """

    key: str
    operations: list["TransformOp"]
    upsert: Union[Unset, bool] = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        operations = []
        for operations_item_data in self.operations:
            operations_item = operations_item_data.to_dict()
            operations.append(operations_item)

        upsert = self.upsert

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
                "operations": operations,
            }
        )
        if upsert is not UNSET:
            field_dict["upsert"] = upsert

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.transform_op import TransformOp

        d = dict(src_dict)
        key = d.pop("key")

        operations = []
        _operations = d.pop("operations")
        for operations_item_data in _operations:
            operations_item = TransformOp.from_dict(operations_item_data)

            operations.append(operations_item)

        upsert = d.pop("upsert", UNSET)

        transform = cls(
            key=key,
            operations=operations,
            upsert=upsert,
        )

        transform.additional_properties = d
        return transform

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
