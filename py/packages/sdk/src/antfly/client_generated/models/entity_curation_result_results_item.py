from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.entity_curation_result_results_item_curation import EntityCurationResultResultsItemCuration
    from ..models.resolver_distinct_pair import ResolverDistinctPair
    from ..models.resolver_redirect import ResolverRedirect


T = TypeVar("T", bound="EntityCurationResultResultsItem")


@_attrs_define
class EntityCurationResultResultsItem:
    """
    Attributes:
        source_table (str):
        resolver (str):
        key_template (str | Unset):
        canonical_key (str | Unset):
        curation (EntityCurationResultResultsItemCuration | Unset):
        revision (int | Unset):
        changed (bool | Unset):
        replay_sequence (int | Unset):
        applied (bool | Unset):
        retired (bool | Unset):
        error (str | Unset):
        redirects (list[ResolverRedirect] | Unset):
        distinct (list[ResolverDistinctPair] | Unset):
        configured_suppressed (bool | Unset):
        runtime_suppressed (bool | Unset):
    """

    source_table: str
    resolver: str
    key_template: str | Unset = UNSET
    canonical_key: str | Unset = UNSET
    curation: EntityCurationResultResultsItemCuration | Unset = UNSET
    revision: int | Unset = UNSET
    changed: bool | Unset = UNSET
    replay_sequence: int | Unset = UNSET
    applied: bool | Unset = UNSET
    retired: bool | Unset = UNSET
    error: str | Unset = UNSET
    redirects: list[ResolverRedirect] | Unset = UNSET
    distinct: list[ResolverDistinctPair] | Unset = UNSET
    configured_suppressed: bool | Unset = UNSET
    runtime_suppressed: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source_table = self.source_table

        resolver = self.resolver

        key_template = self.key_template

        canonical_key = self.canonical_key

        curation: dict[str, Any] | Unset = UNSET
        if not isinstance(self.curation, Unset):
            curation = self.curation.to_dict()

        revision = self.revision

        changed = self.changed

        replay_sequence = self.replay_sequence

        applied = self.applied

        retired = self.retired

        error = self.error

        redirects: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.redirects, Unset):
            redirects = []
            for redirects_item_data in self.redirects:
                redirects_item = redirects_item_data.to_dict()
                redirects.append(redirects_item)

        distinct: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.distinct, Unset):
            distinct = []
            for distinct_item_data in self.distinct:
                distinct_item = distinct_item_data.to_dict()
                distinct.append(distinct_item)

        configured_suppressed = self.configured_suppressed

        runtime_suppressed = self.runtime_suppressed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source_table": source_table,
                "resolver": resolver,
            }
        )
        if key_template is not UNSET:
            field_dict["key_template"] = key_template
        if canonical_key is not UNSET:
            field_dict["canonical_key"] = canonical_key
        if curation is not UNSET:
            field_dict["curation"] = curation
        if revision is not UNSET:
            field_dict["revision"] = revision
        if changed is not UNSET:
            field_dict["changed"] = changed
        if replay_sequence is not UNSET:
            field_dict["replay_sequence"] = replay_sequence
        if applied is not UNSET:
            field_dict["applied"] = applied
        if retired is not UNSET:
            field_dict["retired"] = retired
        if error is not UNSET:
            field_dict["error"] = error
        if redirects is not UNSET:
            field_dict["redirects"] = redirects
        if distinct is not UNSET:
            field_dict["distinct"] = distinct
        if configured_suppressed is not UNSET:
            field_dict["configured_suppressed"] = configured_suppressed
        if runtime_suppressed is not UNSET:
            field_dict["runtime_suppressed"] = runtime_suppressed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.entity_curation_result_results_item_curation import EntityCurationResultResultsItemCuration
        from ..models.resolver_distinct_pair import ResolverDistinctPair
        from ..models.resolver_redirect import ResolverRedirect

        d = dict(src_dict)
        source_table = d.pop("source_table")

        resolver = d.pop("resolver")

        key_template = d.pop("key_template", UNSET)

        canonical_key = d.pop("canonical_key", UNSET)

        _curation = d.pop("curation", UNSET)
        curation: EntityCurationResultResultsItemCuration | Unset
        if isinstance(_curation, Unset):
            curation = UNSET
        else:
            curation = EntityCurationResultResultsItemCuration.from_dict(_curation)

        revision = d.pop("revision", UNSET)

        changed = d.pop("changed", UNSET)

        replay_sequence = d.pop("replay_sequence", UNSET)

        applied = d.pop("applied", UNSET)

        retired = d.pop("retired", UNSET)

        error = d.pop("error", UNSET)

        _redirects = d.pop("redirects", UNSET)
        redirects: list[ResolverRedirect] | Unset = UNSET
        if _redirects is not UNSET:
            redirects = []
            for redirects_item_data in _redirects:
                redirects_item = ResolverRedirect.from_dict(redirects_item_data)

                redirects.append(redirects_item)

        _distinct = d.pop("distinct", UNSET)
        distinct: list[ResolverDistinctPair] | Unset = UNSET
        if _distinct is not UNSET:
            distinct = []
            for distinct_item_data in _distinct:
                distinct_item = ResolverDistinctPair.from_dict(distinct_item_data)

                distinct.append(distinct_item)

        configured_suppressed = d.pop("configured_suppressed", UNSET)

        runtime_suppressed = d.pop("runtime_suppressed", UNSET)

        entity_curation_result_results_item = cls(
            source_table=source_table,
            resolver=resolver,
            key_template=key_template,
            canonical_key=canonical_key,
            curation=curation,
            revision=revision,
            changed=changed,
            replay_sequence=replay_sequence,
            applied=applied,
            retired=retired,
            error=error,
            redirects=redirects,
            distinct=distinct,
            configured_suppressed=configured_suppressed,
            runtime_suppressed=runtime_suppressed,
        )

        entity_curation_result_results_item.additional_properties = d
        return entity_curation_result_results_item

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
