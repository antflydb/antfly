from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_strategy import QueryStrategy
from ..models.semantic_query_mode import SemanticQueryMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="ClassificationStepConfig")


@_attrs_define
class ClassificationStepConfig:
    """Configuration for the classification step. This step analyzes the query,
    selects the optimal retrieval strategy, and generates semantic transformations.

        Attributes:
            enabled (bool | Unset): Compatibility switch. The step is enabled when this object is present; omit the step to
                disable it.
            with_reasoning (bool | Unset): Include pre-retrieval reasoning explaining query analysis and strategy selection
                Default: False.
            force_strategy (QueryStrategy | Unset): Strategy for query transformation and retrieval:
                - simple: Direct query with multi-phrase expansion. Best for straightforward factual queries.
                - decompose: Break complex queries into sub-questions, retrieve for each. Best for multi-part questions.
                - step_back: Generate broader background query first, then specific query. Best for questions needing context.
                - hyde: Generate hypothetical answer document, embed that for retrieval. Best for abstract/conceptual questions.
            force_semantic_mode (SemanticQueryMode | Unset): Mode for semantic query generation:
                - rewrite: Transform query into expanded keywords/concepts optimized for vector search (Level 2 optimization)
                - hypothetical: Generate a hypothetical answer that would appear in relevant documents (HyDE - Level 3
                optimization)
    """

    enabled: bool | Unset = UNSET
    with_reasoning: bool | Unset = False
    force_strategy: QueryStrategy | Unset = UNSET
    force_semantic_mode: SemanticQueryMode | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        with_reasoning = self.with_reasoning

        force_strategy: str | Unset = UNSET
        if not isinstance(self.force_strategy, Unset):
            force_strategy = self.force_strategy.value

        force_semantic_mode: str | Unset = UNSET
        if not isinstance(self.force_semantic_mode, Unset):
            force_semantic_mode = self.force_semantic_mode.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if with_reasoning is not UNSET:
            field_dict["with_reasoning"] = with_reasoning
        if force_strategy is not UNSET:
            field_dict["force_strategy"] = force_strategy
        if force_semantic_mode is not UNSET:
            field_dict["force_semantic_mode"] = force_semantic_mode

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        with_reasoning = d.pop("with_reasoning", UNSET)

        _force_strategy = d.pop("force_strategy", UNSET)
        force_strategy: QueryStrategy | Unset
        if isinstance(_force_strategy, Unset):
            force_strategy = UNSET
        else:
            force_strategy = QueryStrategy(_force_strategy)

        _force_semantic_mode = d.pop("force_semantic_mode", UNSET)
        force_semantic_mode: SemanticQueryMode | Unset
        if isinstance(_force_semantic_mode, Unset):
            force_semantic_mode = UNSET
        else:
            force_semantic_mode = SemanticQueryMode(_force_semantic_mode)

        classification_step_config = cls(
            enabled=enabled,
            with_reasoning=with_reasoning,
            force_strategy=force_strategy,
            force_semantic_mode=force_semantic_mode,
        )

        classification_step_config.additional_properties = d
        return classification_step_config

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
