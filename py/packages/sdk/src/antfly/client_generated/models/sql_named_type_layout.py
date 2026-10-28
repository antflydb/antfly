from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_named_type_layout_kind import SQLNamedTypeLayoutKind

if TYPE_CHECKING:
    from ..models.sql_named_enum_label import SQLNamedEnumLabel
    from ..models.sql_named_type_identity import SQLNamedTypeIdentity


T = TypeVar("T", bound="SQLNamedTypeLayout")


@_attrs_define
class SQLNamedTypeLayout:
    """Server-resolved executable type layout retained by a table schema epoch. Catalog ownership is separate from value
    representation.

        Attributes:
            kind (SQLNamedTypeLayoutKind):
            identity (SQLNamedTypeIdentity): Server-resolved immutable SQL type identity. Origin survives restore remapping
                of local IDs and wire OIDs.
            labels (list[SQLNamedEnumLabel]):
    """

    kind: SQLNamedTypeLayoutKind
    identity: SQLNamedTypeIdentity
    labels: list[SQLNamedEnumLabel]

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        identity = self.identity.to_dict()

        labels = []
        for labels_item_data in self.labels:
            labels_item = labels_item_data.to_dict()
            labels.append(labels_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "identity": identity,
                "labels": labels,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_named_enum_label import SQLNamedEnumLabel
        from ..models.sql_named_type_identity import SQLNamedTypeIdentity

        d = dict(src_dict)
        kind = SQLNamedTypeLayoutKind(d.pop("kind"))

        identity = SQLNamedTypeIdentity.from_dict(d.pop("identity"))

        labels = []
        _labels = d.pop("labels")
        for labels_item_data in _labels:
            labels_item = SQLNamedEnumLabel.from_dict(labels_item_data)

            labels.append(labels_item)

        sql_named_type_layout = cls(
            kind=kind,
            identity=identity,
            labels=labels,
        )

        return sql_named_type_layout
