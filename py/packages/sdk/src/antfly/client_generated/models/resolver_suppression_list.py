from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_resolver_suppression_config import GraphResolverSuppressionConfig


T = TypeVar("T", bound="ResolverSuppressionList")


@_attrs_define
class ResolverSuppressionList:
    """
    Attributes:
        configured (GraphResolverSuppressionConfig): Suppress mentions before candidate search, embedding, adjudication
            and promotion. Suppressed mentions remain explainable in resolution artifacts and contribute no canonical graph
            edges. Changes use the normal resolver generation and bounded backfill lifecycle. Redirects to suppressed keys
            are rejected. Runtime suppressions are additive and outlive index versions.
        runtime (GraphResolverSuppressionConfig): Suppress mentions before candidate search, embedding, adjudication and
            promotion. Suppressed mentions remain explainable in resolution artifacts and contribute no canonical graph
            edges. Changes use the normal resolver generation and bounded backfill lifecycle. Redirects to suppressed keys
            are rejected. Runtime suppressions are additive and outlive index versions.
        revision (int):
    """

    configured: GraphResolverSuppressionConfig
    runtime: GraphResolverSuppressionConfig
    revision: int
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        configured = self.configured.to_dict()

        runtime = self.runtime.to_dict()

        revision = self.revision

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "configured": configured,
                "runtime": runtime,
                "revision": revision,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_resolver_suppression_config import GraphResolverSuppressionConfig

        d = dict(src_dict)
        configured = GraphResolverSuppressionConfig.from_dict(d.pop("configured"))

        runtime = GraphResolverSuppressionConfig.from_dict(d.pop("runtime"))

        revision = d.pop("revision")

        resolver_suppression_list = cls(
            configured=configured,
            runtime=runtime,
            revision=revision,
        )

        resolver_suppression_list.additional_properties = d
        return resolver_suppression_list

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
