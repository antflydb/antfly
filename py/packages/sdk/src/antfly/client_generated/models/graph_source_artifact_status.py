from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_source_artifact_status_format import GraphSourceArtifactStatusFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphSourceArtifactStatus")


@_attrs_define
class GraphSourceArtifactStatus:
    """Configured graph artifact source projected in deterministic precedence order. During the rolling transition,
    identity is available as canonical artifact on new servers or deprecated name on older servers. New servers continue
    emitting deprecated aliases for old clients, but new clients must not require them.

        Attributes:
            path (str):
            format_ (GraphSourceArtifactStatusFormat):
            artifact (str | Unset): Canonical artifact source identity. New servers always populate this field; clients may
                fall back to the deprecated name alias when reading an older server.
            name (str | Unset): Deprecated compatibility alias for artifact.
            materialization_pending (bool | Unset): Deprecated aggregate compatibility projection. This mirrors enclosing
                index catch-up state and is not source-specific.
    """

    path: str
    format_: GraphSourceArtifactStatusFormat
    artifact: str | Unset = UNSET
    name: str | Unset = UNSET
    materialization_pending: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        path = self.path

        format_ = self.format_.value

        artifact = self.artifact

        name = self.name

        materialization_pending = self.materialization_pending

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "path": path,
                "format": format_,
            }
        )
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if name is not UNSET:
            field_dict["name"] = name
        if materialization_pending is not UNSET:
            field_dict["materialization_pending"] = materialization_pending

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        path = d.pop("path")

        format_ = GraphSourceArtifactStatusFormat(d.pop("format"))

        artifact = d.pop("artifact", UNSET)

        name = d.pop("name", UNSET)

        materialization_pending = d.pop("materialization_pending", UNSET)

        graph_source_artifact_status = cls(
            path=path,
            format_=format_,
            artifact=artifact,
            name=name,
            materialization_pending=materialization_pending,
        )

        return graph_source_artifact_status
