from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ReplicationTransformOp")


@_attrs_define
class ReplicationTransformOp:
    """
    Attributes:
        op (str): Transform operation. Standard ops: `$set`, `$unset`, `$inc`, `$push`, `$pull`,
            `$addToSet`, `$pop`, `$mul`, `$min`, `$max`, `$currentDate`, `$rename`.
            Replication-specific: `$merge` (flatten JSONB into top-level fields),
            `$delete_document` (delete entire Antfly doc, `on_delete` only).
             Example: $set.
        path (Union[Unset, str]): Antfly document field path. Required for `$set`, `$unset`, etc. Example: email.
        value (Union[Unset, Any]): Value for the operation. Can be a literal (string, number, boolean)
            or a `{{column}}` reference to a PG column value. Use `{{col.key}}` to
            navigate into decoded JSONB columns.
    """

    op: str
    path: Union[Unset, str] = UNSET
    value: Union[Unset, Any] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        op = self.op

        path = self.path

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "op": op,
            }
        )
        if path is not UNSET:
            field_dict["path"] = path
        if value is not UNSET:
            field_dict["value"] = value

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        op = d.pop("op")

        path = d.pop("path", UNSET)

        value = d.pop("value", UNSET)

        replication_transform_op = cls(
            op=op,
            path=path,
            value=value,
        )

        replication_transform_op.additional_properties = d
        return replication_transform_op

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
