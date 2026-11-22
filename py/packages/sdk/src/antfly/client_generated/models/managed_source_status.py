from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.managed_source_destination_contract import ManagedSourceDestinationContract
    from ..models.managed_source_progress import ManagedSourceProgress
    from ..models.managed_source_status_index_publication_type_0 import ManagedSourceStatusIndexPublicationType0
    from ..models.managed_source_status_replication_sources_item import ManagedSourceStatusReplicationSourcesItem


T = TypeVar("T", bound="ManagedSourceStatus")


@_attrs_define
class ManagedSourceStatus:
    """
    Attributes:
        table_id (int):
        sources_hash (str):
        replication_sources (list[ManagedSourceStatusReplicationSourcesItem]): Source definitions with credential
            material redacted.
        progress (list[ManagedSourceProgress]):
        destination_contract (ManagedSourceDestinationContract):
        cdc_supported (bool):
        cdc_unsupported_reason (str):
        index_publication (ManagedSourceStatusIndexPublicationType0 | None | Unset):
    """

    table_id: int
    sources_hash: str
    replication_sources: list[ManagedSourceStatusReplicationSourcesItem]
    progress: list[ManagedSourceProgress]
    destination_contract: ManagedSourceDestinationContract
    cdc_supported: bool
    cdc_unsupported_reason: str
    index_publication: ManagedSourceStatusIndexPublicationType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.managed_source_status_index_publication_type_0 import ManagedSourceStatusIndexPublicationType0

        table_id = self.table_id

        sources_hash = self.sources_hash

        replication_sources = []
        for replication_sources_item_data in self.replication_sources:
            replication_sources_item = replication_sources_item_data.to_dict()
            replication_sources.append(replication_sources_item)

        progress = []
        for progress_item_data in self.progress:
            progress_item = progress_item_data.to_dict()
            progress.append(progress_item)

        destination_contract = self.destination_contract.to_dict()

        cdc_supported = self.cdc_supported

        cdc_unsupported_reason = self.cdc_unsupported_reason

        index_publication: dict[str, Any] | None | Unset
        if isinstance(self.index_publication, Unset):
            index_publication = UNSET
        elif isinstance(self.index_publication, ManagedSourceStatusIndexPublicationType0):
            index_publication = self.index_publication.to_dict()
        else:
            index_publication = self.index_publication

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "table_id": table_id,
                "sources_hash": sources_hash,
                "replication_sources": replication_sources,
                "progress": progress,
                "destination_contract": destination_contract,
                "cdc_supported": cdc_supported,
                "cdc_unsupported_reason": cdc_unsupported_reason,
            }
        )
        if index_publication is not UNSET:
            field_dict["index_publication"] = index_publication

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.managed_source_destination_contract import ManagedSourceDestinationContract
        from ..models.managed_source_progress import ManagedSourceProgress
        from ..models.managed_source_status_index_publication_type_0 import ManagedSourceStatusIndexPublicationType0
        from ..models.managed_source_status_replication_sources_item import ManagedSourceStatusReplicationSourcesItem

        d = dict(src_dict)
        table_id = d.pop("table_id")

        sources_hash = d.pop("sources_hash")

        replication_sources = []
        _replication_sources = d.pop("replication_sources")
        for replication_sources_item_data in _replication_sources:
            replication_sources_item = ManagedSourceStatusReplicationSourcesItem.from_dict(
                replication_sources_item_data
            )

            replication_sources.append(replication_sources_item)

        progress = []
        _progress = d.pop("progress")
        for progress_item_data in _progress:
            progress_item = ManagedSourceProgress.from_dict(progress_item_data)

            progress.append(progress_item)

        destination_contract = ManagedSourceDestinationContract.from_dict(d.pop("destination_contract"))

        cdc_supported = d.pop("cdc_supported")

        cdc_unsupported_reason = d.pop("cdc_unsupported_reason")

        def _parse_index_publication(data: object) -> ManagedSourceStatusIndexPublicationType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                index_publication_type_0 = ManagedSourceStatusIndexPublicationType0.from_dict(data)

                return index_publication_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ManagedSourceStatusIndexPublicationType0 | None | Unset, data)

        index_publication = _parse_index_publication(d.pop("index_publication", UNSET))

        managed_source_status = cls(
            table_id=table_id,
            sources_hash=sources_hash,
            replication_sources=replication_sources,
            progress=progress,
            destination_contract=destination_contract,
            cdc_supported=cdc_supported,
            cdc_unsupported_reason=cdc_unsupported_reason,
            index_publication=index_publication,
        )

        managed_source_status.additional_properties = d
        return managed_source_status

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
