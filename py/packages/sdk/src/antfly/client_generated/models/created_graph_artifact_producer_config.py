from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.created_graph_artifact_producer_config_kind import CreatedGraphArtifactProducerConfigKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy
    from ..models.graph_artifact_producer_source_config import GraphArtifactProducerSourceConfig


T = TypeVar("T", bound="CreatedGraphArtifactProducerConfig")


@_attrs_define
class CreatedGraphArtifactProducerConfig:
    """Credential-free graph artifact producer configuration returned after creation.

    Attributes:
        name (str):
        kind (CreatedGraphArtifactProducerConfigKind):
        source (GraphArtifactProducerSourceConfig): Document input used by an artifact producer. Field sources read one
            document field; template sources render a Handlebars template.
        content_type (str | Unset):
        execution (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
            operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    name: str
    kind: CreatedGraphArtifactProducerConfigKind
    source: GraphArtifactProducerSourceConfig
    content_type: str | Unset = UNSET
    execution: ExecutionPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        source = self.source.to_dict()

        content_type = self.content_type

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "source": source,
            }
        )
        if content_type is not UNSET:
            field_dict["content_type"] = content_type
        if execution is not UNSET:
            field_dict["execution"] = execution

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.execution_policy import ExecutionPolicy
        from ..models.graph_artifact_producer_source_config import GraphArtifactProducerSourceConfig

        d = dict(src_dict)
        name = d.pop("name")

        kind = CreatedGraphArtifactProducerConfigKind(d.pop("kind"))

        source = GraphArtifactProducerSourceConfig.from_dict(d.pop("source"))

        content_type = d.pop("content_type", UNSET)

        _execution = d.pop("execution", UNSET)
        execution: ExecutionPolicy | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = ExecutionPolicy.from_dict(_execution)

        created_graph_artifact_producer_config = cls(
            name=name,
            kind=kind,
            source=source,
            content_type=content_type,
            execution=execution,
        )

        return created_graph_artifact_producer_config
