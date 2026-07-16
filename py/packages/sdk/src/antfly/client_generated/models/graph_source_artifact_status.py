from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_source_artifact_status_format import GraphSourceArtifactStatusFormat

T = TypeVar("T", bound="GraphSourceArtifactStatus")


@_attrs_define
class GraphSourceArtifactStatus:
    """Materialization status for one configured graph artifact source.

    Attributes:
        name (str): Configured artifact source name.
        path (str): Configured JSON path, or an empty string when the payload root is consumed.
        format_ (GraphSourceArtifactStatusFormat): Payload interpretation configured for this source.
        materialization_pending (bool): Whether index-wide replay or catch-up can still change this source's visible
            graph materialization.
    """

    name: str
    path: str
    format_: GraphSourceArtifactStatusFormat
    materialization_pending: bool

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        path = self.path

        format_ = self.format_.value

        materialization_pending = self.materialization_pending

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "path": path,
                "format": format_,
                "materialization_pending": materialization_pending,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        path = d.pop("path")

        format_ = GraphSourceArtifactStatusFormat(d.pop("format"))

        materialization_pending = d.pop("materialization_pending")

        graph_source_artifact_status = cls(
            name=name,
            path=path,
            format_=format_,
            materialization_pending=materialization_pending,
        )

        return graph_source_artifact_status
