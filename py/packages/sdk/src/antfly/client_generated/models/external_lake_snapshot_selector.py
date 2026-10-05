from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.external_lake_snapshot_selector_mode import ExternalLakeSnapshotSelectorMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExternalLakeSnapshotSelector")


@_attrs_define
class ExternalLakeSnapshotSelector:
    """
    Attributes:
        mode (ExternalLakeSnapshotSelectorMode):
        id (str | Unset): Required for snapshot_id; selects an Iceberg snapshot.
        digest (str | Unset): Required for object_version_digest; pins a Parquet object inventory.
    """

    mode: ExternalLakeSnapshotSelectorMode
    id: str | Unset = UNSET
    digest: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        id = self.id

        digest = self.digest

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "mode": mode,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if digest is not UNSET:
            field_dict["digest"] = digest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mode = ExternalLakeSnapshotSelectorMode(d.pop("mode"))

        id = d.pop("id", UNSET)

        digest = d.pop("digest", UNSET)

        external_lake_snapshot_selector = cls(
            mode=mode,
            id=id,
            digest=digest,
        )

        return external_lake_snapshot_selector
