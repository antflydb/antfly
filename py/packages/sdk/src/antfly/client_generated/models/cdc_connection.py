from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="CdcConnection")


@_attrs_define
class CdcConnection:
    """
    Attributes:
        provider (str): CDC provider type. Currently "postgres"; future CDC providers may add new values. Example:
            postgres.
        table_name (str): Antfly table receiving changes from this CDC source.
        source_ordinal (int): Zero-based ordinal of the replication source within the table config.
        external_table (str | Unset): Source-side table or stream name when reported by the provider.
        slot_name (str | Unset): Provider replication cursor or slot name when applicable.
        publication_name (str | Unset): Provider publication or stream grouping name when applicable.
        phase (str | Unset): Runtime CDC phase such as snapshot, streaming, configured, or failed.
        lag_records (int | Unset): Source records behind, when reported by the runtime.
        lag_millis (int | Unset): Source commit lag in milliseconds, when reported by the runtime.
        last_success_at_ms (int | Unset): Wall-clock timestamp of the last successful CDC poll/apply, in milliseconds.
        last_change_applied_at_ms (int | Unset): Wall-clock timestamp of the last applied source change, in
            milliseconds.
        updated_at_ms (int | Unset): Wall-clock timestamp when this CDC status was last updated, in milliseconds.
    """

    provider: str
    table_name: str
    source_ordinal: int
    external_table: str | Unset = UNSET
    slot_name: str | Unset = UNSET
    publication_name: str | Unset = UNSET
    phase: str | Unset = UNSET
    lag_records: int | Unset = UNSET
    lag_millis: int | Unset = UNSET
    last_success_at_ms: int | Unset = UNSET
    last_change_applied_at_ms: int | Unset = UNSET
    updated_at_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider

        table_name = self.table_name

        source_ordinal = self.source_ordinal

        external_table = self.external_table

        slot_name = self.slot_name

        publication_name = self.publication_name

        phase = self.phase

        lag_records = self.lag_records

        lag_millis = self.lag_millis

        last_success_at_ms = self.last_success_at_ms

        last_change_applied_at_ms = self.last_change_applied_at_ms

        updated_at_ms = self.updated_at_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
                "table_name": table_name,
                "source_ordinal": source_ordinal,
            }
        )
        if external_table is not UNSET:
            field_dict["external_table"] = external_table
        if slot_name is not UNSET:
            field_dict["slot_name"] = slot_name
        if publication_name is not UNSET:
            field_dict["publication_name"] = publication_name
        if phase is not UNSET:
            field_dict["phase"] = phase
        if lag_records is not UNSET:
            field_dict["lag_records"] = lag_records
        if lag_millis is not UNSET:
            field_dict["lag_millis"] = lag_millis
        if last_success_at_ms is not UNSET:
            field_dict["last_success_at_ms"] = last_success_at_ms
        if last_change_applied_at_ms is not UNSET:
            field_dict["last_change_applied_at_ms"] = last_change_applied_at_ms
        if updated_at_ms is not UNSET:
            field_dict["updated_at_ms"] = updated_at_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = d.pop("provider")

        table_name = d.pop("table_name")

        source_ordinal = d.pop("source_ordinal")

        external_table = d.pop("external_table", UNSET)

        slot_name = d.pop("slot_name", UNSET)

        publication_name = d.pop("publication_name", UNSET)

        phase = d.pop("phase", UNSET)

        lag_records = d.pop("lag_records", UNSET)

        lag_millis = d.pop("lag_millis", UNSET)

        last_success_at_ms = d.pop("last_success_at_ms", UNSET)

        last_change_applied_at_ms = d.pop("last_change_applied_at_ms", UNSET)

        updated_at_ms = d.pop("updated_at_ms", UNSET)

        cdc_connection = cls(
            provider=provider,
            table_name=table_name,
            source_ordinal=source_ordinal,
            external_table=external_table,
            slot_name=slot_name,
            publication_name=publication_name,
            phase=phase,
            lag_records=lag_records,
            lag_millis=lag_millis,
            last_success_at_ms=last_success_at_ms,
            last_change_applied_at_ms=last_change_applied_at_ms,
            updated_at_ms=updated_at_ms,
        )

        cdc_connection.additional_properties = d
        return cdc_connection

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
