from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.relational_index_stats_index_type import RelationalIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_rebuild_status import RelationalIndexRebuildStatus
    from ..models.relational_index_repair_status import RelationalIndexRepairStatus


T = TypeVar("T", bound="RelationalIndexStats")


@_attrs_define
class RelationalIndexStats:
    """Public status for schema-backed relational indexes.

    Attributes:
        index_type (RelationalIndexStatsIndexType): Discriminator for the index stats variant.
        access_method (str | Unset):
        lifecycle (str | Unset):
        ready (bool | Unset):
        generation (int | Unset):
        lag (int | Unset): Shared generation-record catch-up lag for the schema-backed access method.
        ready_watermark (int | Unset): Shared generation-record watermark through which the access method is ready.
        schema_fingerprint (None | str | Unset):
        rebuild (RelationalIndexRebuildStatus | Unset): Aggregate secondary-index rebuild progress projected from
            authoritative metadata ranges.
        repair (RelationalIndexRepairStatus | Unset): Aggregate durable repair-job evidence for schema-backed relational
            indexes on this table.
    """

    index_type: RelationalIndexStatsIndexType
    access_method: str | Unset = UNSET
    lifecycle: str | Unset = UNSET
    ready: bool | Unset = UNSET
    generation: int | Unset = UNSET
    lag: int | Unset = UNSET
    ready_watermark: int | Unset = UNSET
    schema_fingerprint: None | str | Unset = UNSET
    rebuild: RelationalIndexRebuildStatus | Unset = UNSET
    repair: RelationalIndexRepairStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        access_method = self.access_method

        lifecycle = self.lifecycle

        ready = self.ready

        generation = self.generation

        lag = self.lag

        ready_watermark = self.ready_watermark

        schema_fingerprint: None | str | Unset
        if isinstance(self.schema_fingerprint, Unset):
            schema_fingerprint = UNSET
        else:
            schema_fingerprint = self.schema_fingerprint

        rebuild: dict[str, Any] | Unset = UNSET
        if not isinstance(self.rebuild, Unset):
            rebuild = self.rebuild.to_dict()

        repair: dict[str, Any] | Unset = UNSET
        if not isinstance(self.repair, Unset):
            repair = self.repair.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if access_method is not UNSET:
            field_dict["access_method"] = access_method
        if lifecycle is not UNSET:
            field_dict["lifecycle"] = lifecycle
        if ready is not UNSET:
            field_dict["ready"] = ready
        if generation is not UNSET:
            field_dict["generation"] = generation
        if lag is not UNSET:
            field_dict["lag"] = lag
        if ready_watermark is not UNSET:
            field_dict["ready_watermark"] = ready_watermark
        if schema_fingerprint is not UNSET:
            field_dict["schema_fingerprint"] = schema_fingerprint
        if rebuild is not UNSET:
            field_dict["rebuild"] = rebuild
        if repair is not UNSET:
            field_dict["repair"] = repair

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_rebuild_status import RelationalIndexRebuildStatus
        from ..models.relational_index_repair_status import RelationalIndexRepairStatus

        d = dict(src_dict)
        index_type = RelationalIndexStatsIndexType(d.pop("index_type"))

        access_method = d.pop("access_method", UNSET)

        lifecycle = d.pop("lifecycle", UNSET)

        ready = d.pop("ready", UNSET)

        generation = d.pop("generation", UNSET)

        lag = d.pop("lag", UNSET)

        ready_watermark = d.pop("ready_watermark", UNSET)

        def _parse_schema_fingerprint(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        schema_fingerprint = _parse_schema_fingerprint(d.pop("schema_fingerprint", UNSET))

        _rebuild = d.pop("rebuild", UNSET)
        rebuild: RelationalIndexRebuildStatus | Unset
        if isinstance(_rebuild, Unset):
            rebuild = UNSET
        else:
            rebuild = RelationalIndexRebuildStatus.from_dict(_rebuild)

        _repair = d.pop("repair", UNSET)
        repair: RelationalIndexRepairStatus | Unset
        if isinstance(_repair, Unset):
            repair = UNSET
        else:
            repair = RelationalIndexRepairStatus.from_dict(_repair)

        relational_index_stats = cls(
            index_type=index_type,
            access_method=access_method,
            lifecycle=lifecycle,
            ready=ready,
            generation=generation,
            lag=lag,
            ready_watermark=ready_watermark,
            schema_fingerprint=schema_fingerprint,
            rebuild=rebuild,
            repair=repair,
        )

        relational_index_stats.additional_properties = d
        return relational_index_stats

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
