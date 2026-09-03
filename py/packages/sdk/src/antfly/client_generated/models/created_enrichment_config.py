from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_kind import EnrichmentKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy


T = TypeVar("T", bound="CreatedEnrichmentConfig")


@_attrs_define
class CreatedEnrichmentConfig:
    """Credential-free normalized enrichment configuration returned after index creation.

    Attributes:
        name (str):
        kind (EnrichmentKind): Managed generated artifact kind.
        field (str | Unset):
        template (str | Unset):
        source_artifact_name (str | Unset):
        expected_dims (int | Unset):
        vector_space (str | Unset): Optional stable model/token-space identifier asserted for this embedding artifact.
        chunk_size (int | Unset):
        chunk_overlap (int | Unset):
        chunker_json (str | Unset):
        full_text_index (bool | Unset):  Default: False.
        content_type (str | Unset):
        execution (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
            operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    name: str
    kind: EnrichmentKind
    field: str | Unset = UNSET
    template: str | Unset = UNSET
    source_artifact_name: str | Unset = UNSET
    expected_dims: int | Unset = UNSET
    vector_space: str | Unset = UNSET
    chunk_size: int | Unset = UNSET
    chunk_overlap: int | Unset = UNSET
    chunker_json: str | Unset = UNSET
    full_text_index: bool | Unset = False
    content_type: str | Unset = UNSET
    execution: ExecutionPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        field = self.field

        template = self.template

        source_artifact_name = self.source_artifact_name

        expected_dims = self.expected_dims

        vector_space = self.vector_space

        chunk_size = self.chunk_size

        chunk_overlap = self.chunk_overlap

        chunker_json = self.chunker_json

        full_text_index = self.full_text_index

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
        if source_artifact_name is not UNSET:
            field_dict["source_artifact_name"] = source_artifact_name
        if expected_dims is not UNSET:
            field_dict["expected_dims"] = expected_dims
        if vector_space is not UNSET:
            field_dict["vector_space"] = vector_space
        if chunk_size is not UNSET:
            field_dict["chunk_size"] = chunk_size
        if chunk_overlap is not UNSET:
            field_dict["chunk_overlap"] = chunk_overlap
        if chunker_json is not UNSET:
            field_dict["chunker_json"] = chunker_json
        if full_text_index is not UNSET:
            field_dict["full_text_index"] = full_text_index
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

        kind = EnrichmentKind(d.pop("kind"))

        field = d.pop("field", UNSET)

        template = d.pop("template", UNSET)

        source_artifact_name = d.pop("source_artifact_name", UNSET)

        expected_dims = d.pop("expected_dims", UNSET)

        vector_space = d.pop("vector_space", UNSET)

        chunk_size = d.pop("chunk_size", UNSET)

        chunk_overlap = d.pop("chunk_overlap", UNSET)

        chunker_json = d.pop("chunker_json", UNSET)

        full_text_index = d.pop("full_text_index", UNSET)

        content_type = d.pop("content_type", UNSET)

        _execution = d.pop("execution", UNSET)
        execution: ExecutionPolicy | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = ExecutionPolicy.from_dict(_execution)

        created_enrichment_config = cls(
            name=name,
            kind=kind,
            field=field,
            template=template,
            source_artifact_name=source_artifact_name,
            expected_dims=expected_dims,
            vector_space=vector_space,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            chunker_json=chunker_json,
            full_text_index=full_text_index,
            content_type=content_type,
            execution=execution,
        )

        return created_enrichment_config
