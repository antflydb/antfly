from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor


T = TypeVar("T", bound="DocumentArtifactTableReprocessRequest")


@_attrs_define
class DocumentArtifactTableReprocessRequest:
    """Bounded request for reprocessing a derived artifact across source rows in key order.

    Attributes:
        from_key (str | Unset): Exclusive lower bound source document key. Use the prior response next_key to continue.
            Default: ''.
        to_key (str | Unset): Inclusive upper bound source document key, or empty for the end of the table/range.
            Default: ''.
        limit (int | Unset): Maximum source rows to scan per shard-local repair pass. Zero uses the server default.
            Default: 100.
        shard_cursors (list[DocumentArtifactReprocessShardCursor] | Unset): Per-shard continuation cursors returned by a
            prior response. When present, distributed repair resumes exactly these shard-local cursors instead of resolving
            a fresh global key span.
    """

    from_key: str | Unset = ""
    to_key: str | Unset = ""
    limit: int | Unset = 100
    shard_cursors: list[DocumentArtifactReprocessShardCursor] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from_key = self.from_key

        to_key = self.to_key

        limit = self.limit

        shard_cursors: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.shard_cursors, Unset):
            shard_cursors = []
            for shard_cursors_item_data in self.shard_cursors:
                shard_cursors_item = shard_cursors_item_data.to_dict()
                shard_cursors.append(shard_cursors_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if from_key is not UNSET:
            field_dict["from_key"] = from_key
        if to_key is not UNSET:
            field_dict["to_key"] = to_key
        if limit is not UNSET:
            field_dict["limit"] = limit
        if shard_cursors is not UNSET:
            field_dict["shard_cursors"] = shard_cursors

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.document_artifact_reprocess_shard_cursor import DocumentArtifactReprocessShardCursor

        d = dict(src_dict)
        from_key = d.pop("from_key", UNSET)

        to_key = d.pop("to_key", UNSET)

        limit = d.pop("limit", UNSET)

        _shard_cursors = d.pop("shard_cursors", UNSET)
        shard_cursors: list[DocumentArtifactReprocessShardCursor] | Unset = UNSET
        if _shard_cursors is not UNSET:
            shard_cursors = []
            for shard_cursors_item_data in _shard_cursors:
                shard_cursors_item = DocumentArtifactReprocessShardCursor.from_dict(shard_cursors_item_data)

                shard_cursors.append(shard_cursors_item)

        document_artifact_table_reprocess_request = cls(
            from_key=from_key,
            to_key=to_key,
            limit=limit,
            shard_cursors=shard_cursors,
        )

        document_artifact_table_reprocess_request.additional_properties = d
        return document_artifact_table_reprocess_request

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
