from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evidence_resource_type_0_kind import MemoryEvidenceResourceType0Kind
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvidenceResourceType0")


@_attrs_define
class MemoryEvidenceResourceType0:
    """
    Attributes:
        table (str):
        table_id (int):
        kind (MemoryEvidenceResourceType0Kind):
        id (str):
        revision (int):
        database (str | Unset):
        namespace (str | Unset):
        storage_name (None | str | Unset):
        digest (None | str | Unset):
    """

    table: str
    table_id: int
    kind: MemoryEvidenceResourceType0Kind
    id: str
    revision: int
    database: str | Unset = UNSET
    namespace: str | Unset = UNSET
    storage_name: None | str | Unset = UNSET
    digest: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        table_id = self.table_id

        kind = self.kind.value

        id = self.id

        revision = self.revision

        database = self.database

        namespace = self.namespace

        storage_name: None | str | Unset
        if isinstance(self.storage_name, Unset):
            storage_name = UNSET
        else:
            storage_name = self.storage_name

        digest: None | str | Unset
        if isinstance(self.digest, Unset):
            digest = UNSET
        else:
            digest = self.digest

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "table": table,
                "table_id": table_id,
                "kind": kind,
                "id": id,
                "revision": revision,
            }
        )
        if database is not UNSET:
            field_dict["database"] = database
        if namespace is not UNSET:
            field_dict["namespace"] = namespace
        if storage_name is not UNSET:
            field_dict["storage_name"] = storage_name
        if digest is not UNSET:
            field_dict["digest"] = digest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table")

        table_id = d.pop("table_id")

        kind = MemoryEvidenceResourceType0Kind(d.pop("kind"))

        id = d.pop("id")

        revision = d.pop("revision")

        database = d.pop("database", UNSET)

        namespace = d.pop("namespace", UNSET)

        def _parse_storage_name(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        storage_name = _parse_storage_name(d.pop("storage_name", UNSET))

        def _parse_digest(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        digest = _parse_digest(d.pop("digest", UNSET))

        memory_evidence_resource_type_0 = cls(
            table=table,
            table_id=table_id,
            kind=kind,
            id=id,
            revision=revision,
            database=database,
            namespace=namespace,
            storage_name=storage_name,
            digest=digest,
        )

        return memory_evidence_resource_type_0
