from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.lsm_storage_status import LsmStorageStatus
    from ..models.vector_source_storage_status import VectorSourceStorageStatus


T = TypeVar("T", bound="StorageStatus")


@_attrs_define
class StorageStatus:
    """
    Attributes:
        source_vectors (VectorSourceStorageStatus | Unset): Source vector payload counters and the last completed
            reclamation observation. Counters reset on process reopen.
        disk_usage (int | Unset): Disk usage in bytes.
        empty (bool | Unset): Whether the table has received data.
        lsm (LsmStorageStatus | Unset): Compact LSM backend operational status. Detailed low-level counters are
            available through metrics.
    """

    source_vectors: VectorSourceStorageStatus | Unset = UNSET
    disk_usage: int | Unset = UNSET
    empty: bool | Unset = UNSET
    lsm: LsmStorageStatus | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source_vectors: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source_vectors, Unset):
            source_vectors = self.source_vectors.to_dict()

        disk_usage = self.disk_usage

        empty = self.empty

        lsm: dict[str, Any] | Unset = UNSET
        if not isinstance(self.lsm, Unset):
            lsm = self.lsm.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if source_vectors is not UNSET:
            field_dict["source_vectors"] = source_vectors
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if empty is not UNSET:
            field_dict["empty"] = empty
        if lsm is not UNSET:
            field_dict["lsm"] = lsm

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.lsm_storage_status import LsmStorageStatus
        from ..models.vector_source_storage_status import VectorSourceStorageStatus

        d = dict(src_dict)
        _source_vectors = d.pop("source_vectors", UNSET)
        source_vectors: VectorSourceStorageStatus | Unset
        if isinstance(_source_vectors, Unset):
            source_vectors = UNSET
        else:
            source_vectors = VectorSourceStorageStatus.from_dict(_source_vectors)

        disk_usage = d.pop("disk_usage", UNSET)

        empty = d.pop("empty", UNSET)

        _lsm = d.pop("lsm", UNSET)
        lsm: LsmStorageStatus | Unset
        if isinstance(_lsm, Unset):
            lsm = UNSET
        else:
            lsm = LsmStorageStatus.from_dict(_lsm)

        storage_status = cls(
            source_vectors=source_vectors,
            disk_usage=disk_usage,
            empty=empty,
            lsm=lsm,
        )

        storage_status.additional_properties = d
        return storage_status

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
