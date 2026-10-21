from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_space_config_features_reconciliation import MemorySpaceConfigFeaturesReconciliation
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorySpaceConfigFeatures")


@_attrs_define
class MemorySpaceConfigFeatures:
    """
    Attributes:
        semantic_recall (bool | Unset):  Default: False.
        entity_extraction (bool | Unset):  Default: False.
        assertion_distillation (bool | Unset):  Default: False.
        summaries (bool | Unset):  Default: False.
        reconciliation (MemorySpaceConfigFeaturesReconciliation | Unset):  Default:
            MemorySpaceConfigFeaturesReconciliation.EXPLICIT.
    """

    semantic_recall: bool | Unset = False
    entity_extraction: bool | Unset = False
    assertion_distillation: bool | Unset = False
    summaries: bool | Unset = False
    reconciliation: MemorySpaceConfigFeaturesReconciliation | Unset = MemorySpaceConfigFeaturesReconciliation.EXPLICIT

    def to_dict(self) -> dict[str, Any]:
        semantic_recall = self.semantic_recall

        entity_extraction = self.entity_extraction

        assertion_distillation = self.assertion_distillation

        summaries = self.summaries

        reconciliation: str | Unset = UNSET
        if not isinstance(self.reconciliation, Unset):
            reconciliation = self.reconciliation.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if semantic_recall is not UNSET:
            field_dict["semantic_recall"] = semantic_recall
        if entity_extraction is not UNSET:
            field_dict["entity_extraction"] = entity_extraction
        if assertion_distillation is not UNSET:
            field_dict["assertion_distillation"] = assertion_distillation
        if summaries is not UNSET:
            field_dict["summaries"] = summaries
        if reconciliation is not UNSET:
            field_dict["reconciliation"] = reconciliation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        semantic_recall = d.pop("semantic_recall", UNSET)

        entity_extraction = d.pop("entity_extraction", UNSET)

        assertion_distillation = d.pop("assertion_distillation", UNSET)

        summaries = d.pop("summaries", UNSET)

        _reconciliation = d.pop("reconciliation", UNSET)
        reconciliation: MemorySpaceConfigFeaturesReconciliation | Unset
        if isinstance(_reconciliation, Unset):
            reconciliation = UNSET
        else:
            reconciliation = MemorySpaceConfigFeaturesReconciliation(_reconciliation)

        memory_space_config_features = cls(
            semantic_recall=semantic_recall,
            entity_extraction=entity_extraction,
            assertion_distillation=assertion_distillation,
            summaries=summaries,
            reconciliation=reconciliation,
        )

        return memory_space_config_features
