from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_index_source_format import GraphIndexSourceFormat
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphIndexSource")


@_attrs_define
class GraphIndexSource:
    """Graph-specific artifact source. The source owns the payload path and format because different artifact streams in
    one graph index may require different interpretations.

        Attributes:
            artifact (str): Stable name of the generated graph artifact.
            path (str | Unset): Optional JSON path selecting edge records within this artifact payload. Example:
                $.relations[*].
            format_ (GraphIndexSourceFormat | Unset): Payload interpretation for this artifact source. Default:
                GraphIndexSourceFormat.EXTRACTION_RELATION.
            mention_edge_type (str | Unset): Optional provenance edge type emitted for resolver mention decisions from this
                source.
    """

    artifact: str
    path: str | Unset = UNSET
    format_: GraphIndexSourceFormat | Unset = GraphIndexSourceFormat.EXTRACTION_RELATION
    mention_edge_type: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        path = self.path

        format_: str | Unset = UNSET
        if not isinstance(self.format_, Unset):
            format_ = self.format_.value

        mention_edge_type = self.mention_edge_type

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
            }
        )
        if path is not UNSET:
            field_dict["path"] = path
        if format_ is not UNSET:
            field_dict["format"] = format_
        if mention_edge_type is not UNSET:
            field_dict["mention_edge_type"] = mention_edge_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        path = d.pop("path", UNSET)

        _format_ = d.pop("format", UNSET)
        format_: GraphIndexSourceFormat | Unset
        if isinstance(_format_, Unset):
            format_ = UNSET
        else:
            format_ = GraphIndexSourceFormat(_format_)

        mention_edge_type = d.pop("mention_edge_type", UNSET)

        graph_index_source = cls(
            artifact=artifact,
            path=path,
            format_=format_,
            mention_edge_type=mention_edge_type,
        )

        return graph_index_source
