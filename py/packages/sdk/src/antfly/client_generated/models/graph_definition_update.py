from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_definition import GraphDefinition


T = TypeVar("T", bound="GraphDefinitionUpdate")


@_attrs_define
class GraphDefinitionUpdate:
    """
    Attributes:
        expected_revision (str):
        definition (GraphDefinition):
    """

    expected_revision: str
    definition: GraphDefinition

    def to_dict(self) -> dict[str, Any]:
        expected_revision = self.expected_revision

        definition = self.definition.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "expected_revision": expected_revision,
                "definition": definition,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_definition import GraphDefinition

        d = dict(src_dict)
        expected_revision = d.pop("expected_revision")

        definition = GraphDefinition.from_dict(d.pop("definition"))

        graph_definition_update = cls(
            expected_revision=expected_revision,
            definition=definition,
        )

        return graph_definition_update
