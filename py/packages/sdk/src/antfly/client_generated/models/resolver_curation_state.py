from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.resolver_curation_state_version import ResolverCurationStateVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_resolver_suppression_config import GraphResolverSuppressionConfig
    from ..models.resolver_distinct_pair import ResolverDistinctPair


T = TypeVar("T", bound="ResolverCurationState")


@_attrs_define
class ResolverCurationState:
    """
    Attributes:
        version (ResolverCurationStateVersion | Unset):
        suppress (GraphResolverSuppressionConfig | Unset): Suppress mentions before candidate search, embedding,
            adjudication and promotion. Suppressed mentions remain explainable in resolution artifacts and contribute no
            canonical graph edges. Changes use the normal resolver generation and bounded backfill lifecycle. Redirects to
            suppressed keys are rejected. Runtime suppressions are additive and outlive index versions.
        distinct (list[ResolverDistinctPair] | Unset):
        removed_aliases (list[str] | Unset): Explicitly restored identities; declarative aliases and promotion
            tombstones cannot recreate the removed redirect.
    """

    version: ResolverCurationStateVersion | Unset = UNSET
    suppress: GraphResolverSuppressionConfig | Unset = UNSET
    distinct: list[ResolverDistinctPair] | Unset = UNSET
    removed_aliases: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version: int | Unset = UNSET
        if not isinstance(self.version, Unset):
            version = self.version.value

        suppress: dict[str, Any] | Unset = UNSET
        if not isinstance(self.suppress, Unset):
            suppress = self.suppress.to_dict()

        distinct: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.distinct, Unset):
            distinct = []
            for distinct_item_data in self.distinct:
                distinct_item = distinct_item_data.to_dict()
                distinct.append(distinct_item)

        removed_aliases: list[str] | Unset = UNSET
        if not isinstance(self.removed_aliases, Unset):
            removed_aliases = self.removed_aliases

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if suppress is not UNSET:
            field_dict["suppress"] = suppress
        if distinct is not UNSET:
            field_dict["distinct"] = distinct
        if removed_aliases is not UNSET:
            field_dict["removed_aliases"] = removed_aliases

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_resolver_suppression_config import GraphResolverSuppressionConfig
        from ..models.resolver_distinct_pair import ResolverDistinctPair

        d = dict(src_dict)
        _version = d.pop("version", UNSET)
        version: ResolverCurationStateVersion | Unset
        if isinstance(_version, Unset):
            version = UNSET
        else:
            version = ResolverCurationStateVersion(_version)

        _suppress = d.pop("suppress", UNSET)
        suppress: GraphResolverSuppressionConfig | Unset
        if isinstance(_suppress, Unset):
            suppress = UNSET
        else:
            suppress = GraphResolverSuppressionConfig.from_dict(_suppress)

        _distinct = d.pop("distinct", UNSET)
        distinct: list[ResolverDistinctPair] | Unset = UNSET
        if _distinct is not UNSET:
            distinct = []
            for distinct_item_data in _distinct:
                distinct_item = ResolverDistinctPair.from_dict(distinct_item_data)

                distinct.append(distinct_item)

        removed_aliases = cast(list[str], d.pop("removed_aliases", UNSET))

        resolver_curation_state = cls(
            version=version,
            suppress=suppress,
            distinct=distinct,
            removed_aliases=removed_aliases,
        )

        resolver_curation_state.additional_properties = d
        return resolver_curation_state

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
