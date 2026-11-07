from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.sql_builtin_type import SQLBuiltinType
from ..models.sql_named_domain_base_kind import SQLNamedDomainBaseKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.sql_named_type_reference import SQLNamedTypeReference


T = TypeVar("T", bound="SQLNamedDomainBase")


@_attrs_define
class SQLNamedDomainBase:
    """Exactly one builtin or named base. The server resolves and verifies the complete base chain before publication.

    Attributes:
        kind (SQLNamedDomainBaseKind):
        builtin (SQLBuiltinType | Unset): Exact PostgreSQL builtin identity for a relational root scalar column.
            SQL array columns use SQLArrayElementType for their element identity.
            Set the JSON Schema property's `x-antfly-sql-type` annotation to one of
            these values. The underlying property type must match. SQL array storage
            is not implied by this annotation. Existing unannotated schemas retain
            their original domains.
            The numeric identity uses an underlying number property and exact
            PostgreSQL NUMERIC semantics, never binary float. Submit finite values
            as JSON numeric lexemes or decimal strings; special values use strings
            NaN, Infinity and -Infinity. Const/enum finite numeric members must be
            JSON numbers, not strings. Bounds and multipleOf are exact decimals.
            Public scalar NUMERIC schemas require reader capability version 22.
            To constrain a NUMERIC scalar or SQL-array column, set the root
            property's `x-antfly-sql-numeric-modifier` annotation to an object with
            precision (1..1000) and signed scale (-1000..1000). The annotation
            requires numeric identity and reader capability version 23. Assignment
            rounds before constraints, indexes and generated dependents; overflow
            rejects the write. Restore verifies stored values without rounding.
        named (SQLNamedTypeReference | Unset): Immutable named-base identity pinned by a domain definition.
    """

    kind: SQLNamedDomainBaseKind
    builtin: SQLBuiltinType | Unset = UNSET
    named: SQLNamedTypeReference | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        builtin: str | Unset = UNSET
        if not isinstance(self.builtin, Unset):
            builtin = self.builtin.value

        named: dict[str, Any] | Unset = UNSET
        if not isinstance(self.named, Unset):
            named = self.named.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
            }
        )
        if builtin is not UNSET:
            field_dict["builtin"] = builtin
        if named is not UNSET:
            field_dict["named"] = named

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.sql_named_type_reference import SQLNamedTypeReference

        d = dict(src_dict)
        kind = SQLNamedDomainBaseKind(d.pop("kind"))

        _builtin = d.pop("builtin", UNSET)
        builtin: SQLBuiltinType | Unset
        if isinstance(_builtin, Unset):
            builtin = UNSET
        else:
            builtin = SQLBuiltinType(_builtin)

        _named = d.pop("named", UNSET)
        named: SQLNamedTypeReference | Unset
        if isinstance(_named, Unset):
            named = UNSET
        else:
            named = SQLNamedTypeReference.from_dict(_named)

        sql_named_domain_base = cls(
            kind=kind,
            builtin=builtin,
            named=named,
        )

        return sql_named_domain_base
