from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_profile_value_verification_plans_item import QueryProfileValueVerificationPlansItem


T = TypeVar("T", bound="QueryProfileValueVerification")


@_attrs_define
class QueryProfileValueVerification:
    """Actual logical value verification work, summed across participating shards; plans describe selected physical fields
    and scan fallback reasons. Budgets remain per shard.

        Attributes:
            documents (int | Unset):
            bytes_ (int | Unset):
            verification_metrics_version (int | Unset): One when every participating shard supplied these verification
                counters; zero means aggregate coverage is incomplete.
            source_read_us (int | Unset): Sum of measured provider-read time in microseconds, including admitted source
                lookup work.
            source_parse_us (int | Unset): Sum of original-value parsing time in microseconds.
            source_batches (int | Unset): Number of completed source read windows.
            source_scalar_reads (int | Unset): Number of required individual source reads.
            projected_sources (int | Unset): Rows observed while considering projected reads, including rows that require a
                full-source fallback.
            stored_field_sources (int | Unset): Rows actually verified from stored-field projections.
            batch_refusals (int | Unset): Source windows refused by allocation admission before bounded fallback.
            batch_peak_bytes (int | Unset): Largest admitted source-window allocation observed on any participating shard;
                not process RSS or a sum of concurrent allocations.
            plans (list[QueryProfileValueVerificationPlansItem] | Unset):
    """

    documents: int | Unset = UNSET
    bytes_: int | Unset = UNSET
    verification_metrics_version: int | Unset = UNSET
    source_read_us: int | Unset = UNSET
    source_parse_us: int | Unset = UNSET
    source_batches: int | Unset = UNSET
    source_scalar_reads: int | Unset = UNSET
    projected_sources: int | Unset = UNSET
    stored_field_sources: int | Unset = UNSET
    batch_refusals: int | Unset = UNSET
    batch_peak_bytes: int | Unset = UNSET
    plans: list[QueryProfileValueVerificationPlansItem] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        documents = self.documents

        bytes_ = self.bytes_

        verification_metrics_version = self.verification_metrics_version

        source_read_us = self.source_read_us

        source_parse_us = self.source_parse_us

        source_batches = self.source_batches

        source_scalar_reads = self.source_scalar_reads

        projected_sources = self.projected_sources

        stored_field_sources = self.stored_field_sources

        batch_refusals = self.batch_refusals

        batch_peak_bytes = self.batch_peak_bytes

        plans: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.plans, Unset):
            plans = []
            for plans_item_data in self.plans:
                plans_item = plans_item_data.to_dict()
                plans.append(plans_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if documents is not UNSET:
            field_dict["documents"] = documents
        if bytes_ is not UNSET:
            field_dict["bytes"] = bytes_
        if verification_metrics_version is not UNSET:
            field_dict["verification_metrics_version"] = verification_metrics_version
        if source_read_us is not UNSET:
            field_dict["source_read_us"] = source_read_us
        if source_parse_us is not UNSET:
            field_dict["source_parse_us"] = source_parse_us
        if source_batches is not UNSET:
            field_dict["source_batches"] = source_batches
        if source_scalar_reads is not UNSET:
            field_dict["source_scalar_reads"] = source_scalar_reads
        if projected_sources is not UNSET:
            field_dict["projected_sources"] = projected_sources
        if stored_field_sources is not UNSET:
            field_dict["stored_field_sources"] = stored_field_sources
        if batch_refusals is not UNSET:
            field_dict["batch_refusals"] = batch_refusals
        if batch_peak_bytes is not UNSET:
            field_dict["batch_peak_bytes"] = batch_peak_bytes
        if plans is not UNSET:
            field_dict["plans"] = plans

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_profile_value_verification_plans_item import QueryProfileValueVerificationPlansItem

        d = dict(src_dict)
        documents = d.pop("documents", UNSET)

        bytes_ = d.pop("bytes", UNSET)

        verification_metrics_version = d.pop("verification_metrics_version", UNSET)

        source_read_us = d.pop("source_read_us", UNSET)

        source_parse_us = d.pop("source_parse_us", UNSET)

        source_batches = d.pop("source_batches", UNSET)

        source_scalar_reads = d.pop("source_scalar_reads", UNSET)

        projected_sources = d.pop("projected_sources", UNSET)

        stored_field_sources = d.pop("stored_field_sources", UNSET)

        batch_refusals = d.pop("batch_refusals", UNSET)

        batch_peak_bytes = d.pop("batch_peak_bytes", UNSET)

        _plans = d.pop("plans", UNSET)
        plans: list[QueryProfileValueVerificationPlansItem] | Unset = UNSET
        if _plans is not UNSET:
            plans = []
            for plans_item_data in _plans:
                plans_item = QueryProfileValueVerificationPlansItem.from_dict(plans_item_data)

                plans.append(plans_item)

        query_profile_value_verification = cls(
            documents=documents,
            bytes_=bytes_,
            verification_metrics_version=verification_metrics_version,
            source_read_us=source_read_us,
            source_parse_us=source_parse_us,
            source_batches=source_batches,
            source_scalar_reads=source_scalar_reads,
            projected_sources=projected_sources,
            stored_field_sources=stored_field_sources,
            batch_refusals=batch_refusals,
            batch_peak_bytes=batch_peak_bytes,
            plans=plans,
        )

        query_profile_value_verification.additional_properties = d
        return query_profile_value_verification

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
