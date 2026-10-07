from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.external_lake_table_source_format import ExternalLakeTableSourceFormat
from ..models.external_lake_table_source_kind import ExternalLakeTableSourceKind
from ..models.external_lake_table_source_object_mutability import ExternalLakeTableSourceObjectMutability
from ..models.external_lake_table_source_write_policy import ExternalLakeTableSourceWritePolicy
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.external_lake_credential_ref import ExternalLakeCredentialRef
    from ..models.external_lake_snapshot_selector import ExternalLakeSnapshotSelector


T = TypeVar("T", bound="ExternalLakeTableSource")


@_attrs_define
class ExternalLakeTableSource:
    """Read-only authoritative Parquet or Iceberg source. A serving statement pins its inventory and object versions before
    returning rows.

        Attributes:
            kind (ExternalLakeTableSourceKind):
            table_id (str):
            format_ (ExternalLakeTableSourceFormat):
            uri (str):
            schema_fingerprint (str | Unset):  Default: 'auto'.
            write_policy (ExternalLakeTableSourceWritePolicy | Unset):  Default:
                ExternalLakeTableSourceWritePolicy.READ_ONLY.
            object_mutability (ExternalLakeTableSourceObjectMutability | Unset): Set immutable only when data files are
                never replaced at an existing URI. Allows authenticated provider-version proofs from retained index generations
                to be reused for unchanged data files. Metadata and delete files are still verified. Default:
                ExternalLakeTableSourceObjectMutability.MUTABLE.
            credentials (ExternalLakeCredentialRef | Unset):
            snapshot (ExternalLakeSnapshotSelector | Unset):
    """

    kind: ExternalLakeTableSourceKind
    table_id: str
    format_: ExternalLakeTableSourceFormat
    uri: str
    schema_fingerprint: str | Unset = "auto"
    write_policy: ExternalLakeTableSourceWritePolicy | Unset = ExternalLakeTableSourceWritePolicy.READ_ONLY
    object_mutability: ExternalLakeTableSourceObjectMutability | Unset = ExternalLakeTableSourceObjectMutability.MUTABLE
    credentials: ExternalLakeCredentialRef | Unset = UNSET
    snapshot: ExternalLakeSnapshotSelector | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        table_id = self.table_id

        format_ = self.format_.value

        uri = self.uri

        schema_fingerprint = self.schema_fingerprint

        write_policy: str | Unset = UNSET
        if not isinstance(self.write_policy, Unset):
            write_policy = self.write_policy.value

        object_mutability: str | Unset = UNSET
        if not isinstance(self.object_mutability, Unset):
            object_mutability = self.object_mutability.value

        credentials: dict[str, Any] | Unset = UNSET
        if not isinstance(self.credentials, Unset):
            credentials = self.credentials.to_dict()

        snapshot: dict[str, Any] | Unset = UNSET
        if not isinstance(self.snapshot, Unset):
            snapshot = self.snapshot.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "table_id": table_id,
                "format": format_,
                "uri": uri,
            }
        )
        if schema_fingerprint is not UNSET:
            field_dict["schema_fingerprint"] = schema_fingerprint
        if write_policy is not UNSET:
            field_dict["write_policy"] = write_policy
        if object_mutability is not UNSET:
            field_dict["object_mutability"] = object_mutability
        if credentials is not UNSET:
            field_dict["credentials"] = credentials
        if snapshot is not UNSET:
            field_dict["snapshot"] = snapshot

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.external_lake_credential_ref import ExternalLakeCredentialRef
        from ..models.external_lake_snapshot_selector import ExternalLakeSnapshotSelector

        d = dict(src_dict)
        kind = ExternalLakeTableSourceKind(d.pop("kind"))

        table_id = d.pop("table_id")

        format_ = ExternalLakeTableSourceFormat(d.pop("format"))

        uri = d.pop("uri")

        schema_fingerprint = d.pop("schema_fingerprint", UNSET)

        _write_policy = d.pop("write_policy", UNSET)
        write_policy: ExternalLakeTableSourceWritePolicy | Unset
        if isinstance(_write_policy, Unset):
            write_policy = UNSET
        else:
            write_policy = ExternalLakeTableSourceWritePolicy(_write_policy)

        _object_mutability = d.pop("object_mutability", UNSET)
        object_mutability: ExternalLakeTableSourceObjectMutability | Unset
        if isinstance(_object_mutability, Unset):
            object_mutability = UNSET
        else:
            object_mutability = ExternalLakeTableSourceObjectMutability(_object_mutability)

        _credentials = d.pop("credentials", UNSET)
        credentials: ExternalLakeCredentialRef | Unset
        if isinstance(_credentials, Unset):
            credentials = UNSET
        else:
            credentials = ExternalLakeCredentialRef.from_dict(_credentials)

        _snapshot = d.pop("snapshot", UNSET)
        snapshot: ExternalLakeSnapshotSelector | Unset
        if isinstance(_snapshot, Unset):
            snapshot = UNSET
        else:
            snapshot = ExternalLakeSnapshotSelector.from_dict(_snapshot)

        external_lake_table_source = cls(
            kind=kind,
            table_id=table_id,
            format_=format_,
            uri=uri,
            schema_fingerprint=schema_fingerprint,
            write_policy=write_policy,
            object_mutability=object_mutability,
            credentials=credentials,
            snapshot=snapshot,
        )

        return external_lake_table_source
