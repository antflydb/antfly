from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_source_artifact_status_format import GraphSourceArtifactStatusFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphSourceArtifactStatus")


@_attrs_define
class GraphSourceArtifactStatus:
    """Configured graph artifact source projected in deterministic precedence order. New servers always populate artifact;
    it remains optional in the transition schema so new clients can read older servers. Deprecated aliases remain
    populated for rolling client compatibility.

        Attributes:
            name (str): Deprecated compatibility alias for artifact.
            path (str):
            format_ (GraphSourceArtifactStatusFormat):
            materialization_pending (bool): Deprecated aggregate compatibility projection. This mirrors enclosing index
                catch-up state and is not source-specific.
            artifact (str | Unset): Canonical artifact source identity. New servers always populate this field; clients may
                fall back to the deprecated name alias when reading an older server.
    """

    name: str
    path: str
    format_: GraphSourceArtifactStatusFormat
    materialization_pending: bool
    artifact: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        path = self.path

        format_ = self.format_.value

        materialization_pending = self.materialization_pending

        artifact = self.artifact

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "path": path,
                "format": format_,
                "materialization_pending": materialization_pending,
            }
        )
        if artifact is not UNSET:
            field_dict["artifact"] = artifact

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        path = d.pop("path")

        format_ = GraphSourceArtifactStatusFormat(d.pop("format"))

        materialization_pending = d.pop("materialization_pending")

        artifact = d.pop("artifact", UNSET)

        graph_source_artifact_status = cls(
            name=name,
            path=path,
            format_=format_,
            materialization_pending=materialization_pending,
            artifact=artifact,
        )

        return graph_source_artifact_status
