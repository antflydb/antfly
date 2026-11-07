from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_named_type_layout_kind import SQLNamedTypeLayoutKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_named_domain_definition import SQLNamedDomainDefinition
    from ..models.sql_named_enum_label import SQLNamedEnumLabel
    from ..models.sql_named_type_identity import SQLNamedTypeIdentity


T = TypeVar("T", bound="SQLNamedTypeLayout")


@_attrs_define
class SQLNamedTypeLayout:
    """Server-resolved executable type layout retained by a table schema epoch. Enum layouts require labels; domain layouts
    require domain. The alternatives are mutually exclusive. Catalog ownership is separate from value representation.

        Attributes:
            kind (SQLNamedTypeLayoutKind):
            identity (SQLNamedTypeIdentity): Server-resolved immutable SQL type identity. Origin survives restore remapping
                of local IDs and wire OIDs.
            labels (list[SQLNamedEnumLabel] | Unset):
            domain (SQLNamedDomainDefinition | Unset): Server-resolved immutable domain contract. Defaults affect future
                assignments; restrictive changes require dependent-owner activation.
    """

    kind: SQLNamedTypeLayoutKind
    identity: SQLNamedTypeIdentity
    labels: list[SQLNamedEnumLabel] | Unset = UNSET
    domain: SQLNamedDomainDefinition | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        identity = self.identity.to_dict()

        labels: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = []
            for labels_item_data in self.labels:
                labels_item = labels_item_data.to_dict()
                labels.append(labels_item)

        domain: dict[str, Any] | Unset = UNSET
        if not isinstance(self.domain, Unset):
            domain = self.domain.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "identity": identity,
            }
        )
        if labels is not UNSET:
            field_dict["labels"] = labels
        if domain is not UNSET:
            field_dict["domain"] = domain

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_named_domain_definition import SQLNamedDomainDefinition
        from ..models.sql_named_enum_label import SQLNamedEnumLabel
        from ..models.sql_named_type_identity import SQLNamedTypeIdentity

        d = dict(src_dict)
        kind = SQLNamedTypeLayoutKind(d.pop("kind"))

        identity = SQLNamedTypeIdentity.from_dict(d.pop("identity"))

        _labels = d.pop("labels", UNSET)
        labels: list[SQLNamedEnumLabel] | Unset = UNSET
        if _labels is not UNSET:
            labels = []
            for labels_item_data in _labels:
                labels_item = SQLNamedEnumLabel.from_dict(labels_item_data)

                labels.append(labels_item)

        _domain = d.pop("domain", UNSET)
        domain: SQLNamedDomainDefinition | Unset
        if isinstance(_domain, Unset):
            domain = UNSET
        else:
            domain = SQLNamedDomainDefinition.from_dict(_domain)

        sql_named_type_layout = cls(
            kind=kind,
            identity=identity,
            labels=labels,
            domain=domain,
        )

        return sql_named_type_layout
