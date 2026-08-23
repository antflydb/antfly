from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.hierarchy_artifact_kind import HierarchyArtifactKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_artifact_source import HierarchyArtifactSource


T = TypeVar("T", bound="HierarchyArtifact")


@_attrs_define
class HierarchyArtifact:
    """
    Attributes:
        name (str):
        kind (HierarchyArtifactKind):
        chunk_id (int | Unset):
        unit_id (str | Unset):
        source (HierarchyArtifactSource | Unset):
    """

    name: str
    kind: HierarchyArtifactKind
    chunk_id: int | Unset = UNSET
    unit_id: str | Unset = UNSET
    source: HierarchyArtifactSource | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        chunk_id = self.chunk_id

        unit_id = self.unit_id

        source: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source, Unset):
            source = self.source.to_dict()

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
        if source is not UNSET:
            field_dict["source"] = source

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_artifact_source import HierarchyArtifactSource

        d = dict(src_dict)
        name = d.pop("name")

        kind = HierarchyArtifactKind(d.pop("kind"))

        chunk_id = d.pop("chunk_id", UNSET)

        unit_id = d.pop("unit_id", UNSET)

        _source = d.pop("source", UNSET)
        source: HierarchyArtifactSource | Unset
        if isinstance(_source, Unset):
            source = UNSET
        else:
            source = HierarchyArtifactSource.from_dict(_source)

        hierarchy_artifact = cls(
            name=name,
            kind=kind,
            chunk_id=chunk_id,
            unit_id=unit_id,
            source=source,
        )

        return hierarchy_artifact
