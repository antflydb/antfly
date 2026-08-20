from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_kind import EnrichmentKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_artifact_producer_config_producer_json import GraphArtifactProducerConfigProducerJson


T = TypeVar("T", bound="GraphArtifactProducerConfig")


@_attrs_define
class GraphArtifactProducerConfig:
    """Asset producer used by an artifact-backed graph index.

    Attributes:
        name (str):
        kind (EnrichmentKind): Managed generated artifact kind.
        field (str | Unset):
        content_type (str | Unset):
        producer_json (GraphArtifactProducerConfigProducerJson | Unset): Write-only producer configuration; it may
            contain credentials and is never returned.
    """

    name: str
    kind: EnrichmentKind
    field: str | Unset = UNSET
    content_type: str | Unset = UNSET
    producer_json: GraphArtifactProducerConfigProducerJson | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        field = self.field

        content_type = self.content_type

        producer_json: dict[str, Any] | Unset = UNSET
        if not isinstance(self.producer_json, Unset):
            producer_json = self.producer_json.to_dict()

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
        if producer_json is not UNSET:
            field_dict["producer_json"] = producer_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_artifact_producer_config_producer_json import GraphArtifactProducerConfigProducerJson

        d = dict(src_dict)
        name = d.pop("name")

        kind = EnrichmentKind(d.pop("kind"))

        field = d.pop("field", UNSET)

        content_type = d.pop("content_type", UNSET)

        _producer_json = d.pop("producer_json", UNSET)
        producer_json: GraphArtifactProducerConfigProducerJson | Unset
        if isinstance(_producer_json, Unset):
            producer_json = UNSET
        else:
            producer_json = GraphArtifactProducerConfigProducerJson.from_dict(_producer_json)

        graph_artifact_producer_config = cls(
            name=name,
            kind=kind,
            field=field,
            content_type=content_type,
            producer_json=producer_json,
        )

        return graph_artifact_producer_config
