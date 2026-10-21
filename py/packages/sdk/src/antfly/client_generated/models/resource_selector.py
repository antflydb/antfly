from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.resource_selector_kind import ResourceSelectorKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ResourceSelector")


@_attrs_define
class ResourceSelector:
    """
    Attributes:
        table (str):
        id (str):
        database (str | Unset):
        namespace (str | Unset):
        kind (ResourceSelectorKind | Unset):
        expected_revision (int | None | Unset):
    """

    table: str
    id: str
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET
    kind: ResourceSelectorKind | Unset = UNSET
    expected_revision: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        id = self.id

        database = self.database

        namespace = self.namespace

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        expected_revision: int | None | Unset
        if isinstance(self.expected_revision, Unset):
            expected_revision = UNSET
        else:
            expected_revision = self.expected_revision

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
                "id": id,
            }
        )
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if kind is not UNSET:
            field_dict["kind"] = kind
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        id = d.pop("id")

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        _kind = d.pop("kind", UNSET)
        kind: ResourceSelectorKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ResourceSelectorKind(_kind)

        def _parse_expected_revision(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        expected_revision = _parse_expected_revision(d.pop("expected_revision", UNSET))

        resource_selector = cls(
            table=table,
            id=id,
            database=database,
            namespace=namespace,
            kind=kind,
            expected_revision=expected_revision,
        )

        return resource_selector
