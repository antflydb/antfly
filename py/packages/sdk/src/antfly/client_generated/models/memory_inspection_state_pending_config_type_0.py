from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_inspection_state_pending_config_type_0_ontology_pack import (
    MemoryInspectionStatePendingConfigType0OntologyPack,
)
from ..models.memory_inspection_state_pending_config_type_0_processing import (
    MemoryInspectionStatePendingConfigType0Processing,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_inspection_state_pending_config_type_0_features import (
        MemoryInspectionStatePendingConfigType0Features,
    )
    from ..models.memory_inspection_state_pending_config_type_0_models import (
        MemoryInspectionStatePendingConfigType0Models,
    )
    from ..models.memory_inspection_state_pending_config_type_0_predicates_item import (
        MemoryInspectionStatePendingConfigType0PredicatesItem,
    )
    from ..models.memory_inspection_state_pending_config_type_0_semantic_discovery import (
        MemoryInspectionStatePendingConfigType0SemanticDiscovery,
    )
    from ..models.memory_inspection_state_pending_config_type_0_source_rules_item import (
        MemoryInspectionStatePendingConfigType0SourceRulesItem,
    )


T = TypeVar("T", bound="MemoryInspectionStatePendingConfigType0")


@_attrs_define
class MemoryInspectionStatePendingConfigType0:
    """
    Attributes:
        version (int | Unset):  Default: 1.
        generation (int | Unset):  Default: 1.
        embedding_generation (int | Unset):  Default: 1.
        processing (MemoryInspectionStatePendingConfigType0Processing | Unset):  Default:
            MemoryInspectionStatePendingConfigType0Processing.EXTERNAL.
        ontology_version (str | Unset):
        policy_version (str | Unset):
        features (MemoryInspectionStatePendingConfigType0Features | Unset):
        models (MemoryInspectionStatePendingConfigType0Models | Unset):
        semantic_discovery (MemoryInspectionStatePendingConfigType0SemanticDiscovery | Unset):
        ontology_pack (MemoryInspectionStatePendingConfigType0OntologyPack | Unset):  Default:
            MemoryInspectionStatePendingConfigType0OntologyPack.NONE.
        predicates (list[MemoryInspectionStatePendingConfigType0PredicatesItem] | Unset):
        source_rules (list[MemoryInspectionStatePendingConfigType0SourceRulesItem] | Unset):
        max_input_bytes (int | Unset):  Default: 262144.
        max_output_items (int | Unset):  Default: 128.
        max_attempts (int | Unset):  Default: 3.
        max_recall_tokens (int | Unset):  Default: 8192.
        retention_ms (int | None | Unset):
    """

    version: int | Unset = 1
    generation: int | Unset = 1
    embedding_generation: int | Unset = 1
    processing: MemoryInspectionStatePendingConfigType0Processing | Unset = (
        MemoryInspectionStatePendingConfigType0Processing.EXTERNAL
    )
    ontology_version: str | Unset = UNSET
    policy_version: str | Unset = UNSET
    features: MemoryInspectionStatePendingConfigType0Features | Unset = UNSET
    models: MemoryInspectionStatePendingConfigType0Models | Unset = UNSET
    semantic_discovery: MemoryInspectionStatePendingConfigType0SemanticDiscovery | Unset = UNSET
    ontology_pack: MemoryInspectionStatePendingConfigType0OntologyPack | Unset = (
        MemoryInspectionStatePendingConfigType0OntologyPack.NONE
    )
    predicates: list[MemoryInspectionStatePendingConfigType0PredicatesItem] | Unset = UNSET
    source_rules: list[MemoryInspectionStatePendingConfigType0SourceRulesItem] | Unset = UNSET
    max_input_bytes: int | Unset = 262144
    max_output_items: int | Unset = 128
    max_attempts: int | Unset = 3
    max_recall_tokens: int | Unset = 8192
    retention_ms: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        generation = self.generation

        embedding_generation = self.embedding_generation

        processing: str | Unset = UNSET
        if not isinstance(self.processing, Unset):
            processing = self.processing.value

        ontology_version = self.ontology_version

        policy_version = self.policy_version

        features: dict[str, Any] | Unset = UNSET
        if not isinstance(self.features, Unset):
            features = self.features.to_dict()

        models: dict[str, Any] | Unset = UNSET
        if not isinstance(self.models, Unset):
            models = self.models.to_dict()

        semantic_discovery: dict[str, Any] | Unset = UNSET
        if not isinstance(self.semantic_discovery, Unset):
            semantic_discovery = self.semantic_discovery.to_dict()

        ontology_pack: str | Unset = UNSET
        if not isinstance(self.ontology_pack, Unset):
            ontology_pack = self.ontology_pack.value

        predicates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.predicates, Unset):
            predicates = []
            for predicates_item_data in self.predicates:
                predicates_item = predicates_item_data.to_dict()
                predicates.append(predicates_item)

        source_rules: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.source_rules, Unset):
            source_rules = []
            for source_rules_item_data in self.source_rules:
                source_rules_item = source_rules_item_data.to_dict()
                source_rules.append(source_rules_item)

        max_input_bytes = self.max_input_bytes

        max_output_items = self.max_output_items

        max_attempts = self.max_attempts

        max_recall_tokens = self.max_recall_tokens

        retention_ms: int | None | Unset
        if isinstance(self.retention_ms, Unset):
            retention_ms = UNSET
        else:
            retention_ms = self.retention_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if generation is not UNSET:
            field_dict["generation"] = generation
        if embedding_generation is not UNSET:
            field_dict["embedding_generation"] = embedding_generation
        if processing is not UNSET:
            field_dict["processing"] = processing
        if ontology_version is not UNSET:
            field_dict["ontology_version"] = ontology_version
        if policy_version is not UNSET:
            field_dict["policy_version"] = policy_version
        if features is not UNSET:
            field_dict["features"] = features
        if models is not UNSET:
            field_dict["models"] = models
        if semantic_discovery is not UNSET:
            field_dict["semantic_discovery"] = semantic_discovery
        if ontology_pack is not UNSET:
            field_dict["ontology_pack"] = ontology_pack
        if predicates is not UNSET:
            field_dict["predicates"] = predicates
        if source_rules is not UNSET:
            field_dict["source_rules"] = source_rules
        if max_input_bytes is not UNSET:
            field_dict["max_input_bytes"] = max_input_bytes
        if max_output_items is not UNSET:
            field_dict["max_output_items"] = max_output_items
        if max_attempts is not UNSET:
            field_dict["max_attempts"] = max_attempts
        if max_recall_tokens is not UNSET:
            field_dict["max_recall_tokens"] = max_recall_tokens
        if retention_ms is not UNSET:
            field_dict["retention_ms"] = retention_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_inspection_state_pending_config_type_0_features import (
            MemoryInspectionStatePendingConfigType0Features,
        )
        from ..models.memory_inspection_state_pending_config_type_0_models import (
            MemoryInspectionStatePendingConfigType0Models,
        )
        from ..models.memory_inspection_state_pending_config_type_0_predicates_item import (
            MemoryInspectionStatePendingConfigType0PredicatesItem,
        )
        from ..models.memory_inspection_state_pending_config_type_0_semantic_discovery import (
            MemoryInspectionStatePendingConfigType0SemanticDiscovery,
        )
        from ..models.memory_inspection_state_pending_config_type_0_source_rules_item import (
            MemoryInspectionStatePendingConfigType0SourceRulesItem,
        )

        d = dict(src_dict)
        version = d.pop("version", UNSET)

        generation = d.pop("generation", UNSET)

        embedding_generation = d.pop("embedding_generation", UNSET)

        _processing = d.pop("processing", UNSET)
        processing: MemoryInspectionStatePendingConfigType0Processing | Unset
        if isinstance(_processing, Unset):
            processing = UNSET
        else:
            processing = MemoryInspectionStatePendingConfigType0Processing(_processing)

        ontology_version = d.pop("ontology_version", UNSET)

        policy_version = d.pop("policy_version", UNSET)

        _features = d.pop("features", UNSET)
        features: MemoryInspectionStatePendingConfigType0Features | Unset
        if isinstance(_features, Unset):
            features = UNSET
        else:
            features = MemoryInspectionStatePendingConfigType0Features.from_dict(_features)

        _models = d.pop("models", UNSET)
        models: MemoryInspectionStatePendingConfigType0Models | Unset
        if isinstance(_models, Unset):
            models = UNSET
        else:
            models = MemoryInspectionStatePendingConfigType0Models.from_dict(_models)

        _semantic_discovery = d.pop("semantic_discovery", UNSET)
        semantic_discovery: MemoryInspectionStatePendingConfigType0SemanticDiscovery | Unset
        if isinstance(_semantic_discovery, Unset):
            semantic_discovery = UNSET
        else:
            semantic_discovery = MemoryInspectionStatePendingConfigType0SemanticDiscovery.from_dict(_semantic_discovery)

        _ontology_pack = d.pop("ontology_pack", UNSET)
        ontology_pack: MemoryInspectionStatePendingConfigType0OntologyPack | Unset
        if isinstance(_ontology_pack, Unset):
            ontology_pack = UNSET
        else:
            ontology_pack = MemoryInspectionStatePendingConfigType0OntologyPack(_ontology_pack)

        _predicates = d.pop("predicates", UNSET)
        predicates: list[MemoryInspectionStatePendingConfigType0PredicatesItem] | Unset = UNSET
        if _predicates is not UNSET:
            predicates = []
            for predicates_item_data in _predicates:
                predicates_item = MemoryInspectionStatePendingConfigType0PredicatesItem.from_dict(predicates_item_data)

                predicates.append(predicates_item)

        _source_rules = d.pop("source_rules", UNSET)
        source_rules: list[MemoryInspectionStatePendingConfigType0SourceRulesItem] | Unset = UNSET
        if _source_rules is not UNSET:
            source_rules = []
            for source_rules_item_data in _source_rules:
                source_rules_item = MemoryInspectionStatePendingConfigType0SourceRulesItem.from_dict(
                    source_rules_item_data
                )

                source_rules.append(source_rules_item)

        max_input_bytes = d.pop("max_input_bytes", UNSET)

        max_output_items = d.pop("max_output_items", UNSET)

        max_attempts = d.pop("max_attempts", UNSET)

        max_recall_tokens = d.pop("max_recall_tokens", UNSET)

        def _parse_retention_ms(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        retention_ms = _parse_retention_ms(d.pop("retention_ms", UNSET))

        memory_inspection_state_pending_config_type_0 = cls(
            version=version,
            generation=generation,
            embedding_generation=embedding_generation,
            processing=processing,
            ontology_version=ontology_version,
            policy_version=policy_version,
            features=features,
            models=models,
            semantic_discovery=semantic_discovery,
            ontology_pack=ontology_pack,
            predicates=predicates,
            source_rules=source_rules,
            max_input_bytes=max_input_bytes,
            max_output_items=max_output_items,
            max_attempts=max_attempts,
            max_recall_tokens=max_recall_tokens,
            retention_ms=retention_ms,
        )

        return memory_inspection_state_pending_config_type_0
