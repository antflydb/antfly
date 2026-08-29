from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="FullTextArtifactIndexSource")


@_attrs_define
class FullTextArtifactIndexSource:
    """Textual artifact stream consumed by a full-text index, with an optional source-local projection.

    Attributes:
        artifact (str): Stable name of a chunk or textual asset artifact stream. Artifact names must be unique within
            the index.
        field (str | Unset): Optional field selected from this artifact's records. When omitted, the index-level field
            is inherited; when both are omitted, Antfly indexes the default text projection.
    """

    artifact: str
    field: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        field = self.field

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        field = d.pop("field", UNSET)

        full_text_artifact_index_source = cls(
            artifact=artifact,
            field=field,
        )

        return full_text_artifact_index_source
