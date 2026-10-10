from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.lake_catalog_response_state import LakeCatalogResponseState
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.lake_catalog_response_metadata import LakeCatalogResponseMetadata


T = TypeVar("T", bound="LakeCatalogResponse")


@_attrs_define
class LakeCatalogResponse:
    """
    Attributes:
        state (LakeCatalogResponseState):
        commit_id (str | Unset):
        request_hash (str | Unset): Opaque request digest for outcome resolution.
        metadata_location (str | Unset):
        metadata (LakeCatalogResponseMetadata | Unset):
        binding_ready (bool | Unset): False if lake creation committed but native schema binding still needs the same
            initialization request replayed.
        searchable (bool | Unset): A catalog commit alone does not make a matching index publication searchable.
    """

    state: LakeCatalogResponseState
    commit_id: str | Unset = UNSET
    request_hash: str | Unset = UNSET
    metadata_location: str | Unset = UNSET
    metadata: LakeCatalogResponseMetadata | Unset = UNSET
    binding_ready: bool | Unset = UNSET
    searchable: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        commit_id = self.commit_id

        request_hash = self.request_hash

        metadata_location = self.metadata_location

        metadata: dict[str, Any] | Unset = UNSET
        if not isinstance(self.metadata, Unset):
            metadata = self.metadata.to_dict()

        binding_ready = self.binding_ready

        searchable = self.searchable

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
            }
        )
        if commit_id is not UNSET:
            field_dict["commit_id"] = commit_id
        if request_hash is not UNSET:
            field_dict["request_hash"] = request_hash
        if metadata_location is not UNSET:
            field_dict["metadata_location"] = metadata_location
        if metadata is not UNSET:
            field_dict["metadata"] = metadata
        if binding_ready is not UNSET:
            field_dict["binding_ready"] = binding_ready
        if searchable is not UNSET:
            field_dict["searchable"] = searchable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.lake_catalog_response_metadata import LakeCatalogResponseMetadata

        d = dict(src_dict)
        state = LakeCatalogResponseState(d.pop("state"))

        commit_id = d.pop("commit_id", UNSET)

        request_hash = d.pop("request_hash", UNSET)

        metadata_location = d.pop("metadata_location", UNSET)

        _metadata = d.pop("metadata", UNSET)
        metadata: LakeCatalogResponseMetadata | Unset
        if isinstance(_metadata, Unset):
            metadata = UNSET
        else:
            metadata = LakeCatalogResponseMetadata.from_dict(_metadata)

        binding_ready = d.pop("binding_ready", UNSET)

        searchable = d.pop("searchable", UNSET)

        lake_catalog_response = cls(
            state=state,
            commit_id=commit_id,
            request_hash=request_hash,
            metadata_location=metadata_location,
            metadata=metadata,
            binding_ready=binding_ready,
            searchable=searchable,
        )

        lake_catalog_response.additional_properties = d
        return lake_catalog_response

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
