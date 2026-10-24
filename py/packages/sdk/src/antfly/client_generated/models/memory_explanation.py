from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_explanation_justification import MemoryExplanationJustification
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_explanation_nodes_item import MemoryExplanationNodesItem


T = TypeVar("T", bound="MemoryExplanation")


@_attrs_define
class MemoryExplanation:
    """
    Attributes:
        nodes (list[MemoryExplanationNodesItem]):
        complete (bool):
        truncated (bool):
        missing_evidence (bool):
        schema_version (int | Unset):  Default: 1.
        incomplete_history (bool | Unset):  Default: False.
        justification (MemoryExplanationJustification | Unset):  Default: MemoryExplanationJustification.RECORDED.
    """

    nodes: list[MemoryExplanationNodesItem]
    complete: bool
    truncated: bool
    missing_evidence: bool
    schema_version: int | Unset = 1
    incomplete_history: bool | Unset = False
    justification: MemoryExplanationJustification | Unset = MemoryExplanationJustification.RECORDED

    def to_dict(self) -> dict[str, Any]:
        nodes = []
        for nodes_item_data in self.nodes:
            nodes_item = nodes_item_data.to_dict()
            nodes.append(nodes_item)

        complete = self.complete

        truncated = self.truncated

        missing_evidence = self.missing_evidence

        schema_version = self.schema_version

        incomplete_history = self.incomplete_history

        justification: str | Unset = UNSET
        if not isinstance(self.justification, Unset):
            justification = self.justification.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "nodes": nodes,
                "complete": complete,
                "truncated": truncated,
                "missing_evidence": missing_evidence,
            }
        )
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if incomplete_history is not UNSET:
            field_dict["incomplete_history"] = incomplete_history
        if justification is not UNSET:
            field_dict["justification"] = justification

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_explanation_nodes_item import MemoryExplanationNodesItem

        d = dict(src_dict)
        nodes = []
        _nodes = d.pop("nodes")
        for nodes_item_data in _nodes:
            nodes_item = MemoryExplanationNodesItem.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        complete = d.pop("complete")

        truncated = d.pop("truncated")

        missing_evidence = d.pop("missing_evidence")

        schema_version = d.pop("schema_version", UNSET)

        incomplete_history = d.pop("incomplete_history", UNSET)

        _justification = d.pop("justification", UNSET)
        justification: MemoryExplanationJustification | Unset
        if isinstance(_justification, Unset):
            justification = UNSET
        else:
            justification = MemoryExplanationJustification(_justification)

        memory_explanation = cls(
            nodes=nodes,
            complete=complete,
            truncated=truncated,
            missing_evidence=missing_evidence,
            schema_version=schema_version,
            incomplete_history=incomplete_history,
            justification=justification,
        )

        return memory_explanation
