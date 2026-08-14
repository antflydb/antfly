from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_artifact_source_kind import HierarchyArtifactSourceKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="HierarchyArtifactSource")


@_attrs_define
class HierarchyArtifactSource:
    """
    Attributes:
        name (str):
        kind (HierarchyArtifactSourceKind):
        chunk_id (int | Unset):
        unit_id (str | Unset):
    """

    name: str
    kind: HierarchyArtifactSourceKind
    chunk_id: int | Unset = UNSET
    unit_id: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        chunk_id = self.chunk_id

        unit_id = self.unit_id

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "kind": kind,
            }
        )
        if chunk_id is not UNSET:
            field_dict["chunk_id"] = chunk_id
        if unit_id is not UNSET:
            field_dict["unit_id"] = unit_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        kind = HierarchyArtifactSourceKind(d.pop("kind"))

        chunk_id = d.pop("chunk_id", UNSET)

        unit_id = d.pop("unit_id", UNSET)

        hierarchy_artifact_source = cls(
            name=name,
            kind=kind,
            chunk_id=chunk_id,
            unit_id=unit_id,
        )

        return hierarchy_artifact_source
