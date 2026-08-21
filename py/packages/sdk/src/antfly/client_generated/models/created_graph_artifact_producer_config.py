from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.created_graph_artifact_producer_config_kind import CreatedGraphArtifactProducerConfigKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy


T = TypeVar("T", bound="CreatedGraphArtifactProducerConfig")


@_attrs_define
class CreatedGraphArtifactProducerConfig:
    """Credential-free graph artifact producer configuration returned after creation. At least one non-empty field or
    template source is present.

        Attributes:
            name (str):
            kind (CreatedGraphArtifactProducerConfigKind):
            field (str | Unset):
            template (str | Unset):
            content_type (str | Unset):
            execution (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    name: str
    kind: CreatedGraphArtifactProducerConfigKind
    field: str | Unset = UNSET
    template: str | Unset = UNSET
    content_type: str | Unset = UNSET
    execution: ExecutionPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        field = self.field

        template = self.template

        content_type = self.content_type

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "kind": kind,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if template is not UNSET:
            field_dict["template"] = template
        if content_type is not UNSET:
            field_dict["content_type"] = content_type
        if execution is not UNSET:
            field_dict["execution"] = execution

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.execution_policy import ExecutionPolicy

        d = dict(src_dict)
        name = d.pop("name")

        kind = CreatedGraphArtifactProducerConfigKind(d.pop("kind"))

        field = d.pop("field", UNSET)

        template = d.pop("template", UNSET)

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
            field=field,
            template=template,
            content_type=content_type,
            execution=execution,
        )

        return created_graph_artifact_producer_config
