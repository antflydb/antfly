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
        window_index (int | Unset): Independent media output sub-window within the parent chunk; omitted for an unsplit
            vector.
        unit_id (str | Unset):
        revision (str | Unset): Opaque fingerprint of this member's native content and captured producer/input state;
            independent of hierarchy traversal cursors.
        derivation_revision (str | Unset): Opaque fingerprint of the captured producer/input state, when available.
            Changes to producer configuration or acquired input reopen this identity.
        source (HierarchyArtifactSource | Unset):
    """

    name: str
    kind: HierarchyArtifactKind
    chunk_id: int | Unset = UNSET
    window_index: int | Unset = UNSET
    unit_id: str | Unset = UNSET
    revision: str | Unset = UNSET
    derivation_revision: str | Unset = UNSET
    source: HierarchyArtifactSource | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        chunk_id = self.chunk_id

        window_index = self.window_index

        unit_id = self.unit_id

        revision = self.revision

        derivation_revision = self.derivation_revision

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
        if window_index is not UNSET:
            field_dict["window_index"] = window_index
        if unit_id is not UNSET:
            field_dict["unit_id"] = unit_id
        if revision is not UNSET:
            field_dict["revision"] = revision
        if derivation_revision is not UNSET:
            field_dict["derivation_revision"] = derivation_revision
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

        window_index = d.pop("window_index", UNSET)

        unit_id = d.pop("unit_id", UNSET)

        revision = d.pop("revision", UNSET)

        derivation_revision = d.pop("derivation_revision", UNSET)

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
            window_index=window_index,
            unit_id=unit_id,
            revision=revision,
            derivation_revision=derivation_revision,
            source=source,
        )

        return hierarchy_artifact
