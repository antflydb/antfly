from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_kind import EnrichmentKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="CreatedGraphArtifactProducerConfig")


@_attrs_define
class CreatedGraphArtifactProducerConfig:
    """Credential-free graph artifact producer configuration returned after creation.

    Attributes:
        name (str):
        kind (EnrichmentKind): Managed generated artifact kind.
        field (str | Unset):
        content_type (str | Unset):
    """

    name: str
    kind: EnrichmentKind
    field: str | Unset = UNSET
    content_type: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        field = self.field

        content_type = self.content_type

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "kind": kind,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if content_type is not UNSET:
            field_dict["content_type"] = content_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        kind = EnrichmentKind(d.pop("kind"))

        field = d.pop("field", UNSET)

        content_type = d.pop("content_type", UNSET)

        created_graph_artifact_producer_config = cls(
            name=name,
            kind=kind,
            field=field,
            content_type=content_type,
        )

        return created_graph_artifact_producer_config
