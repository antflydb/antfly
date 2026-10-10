from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.ingest_lake_changes_body_changes_item import IngestLakeChangesBodyChangesItem


T = TypeVar("T", bound="IngestLakeChangesBody")


@_attrs_define
class IngestLakeChangesBody:
    """
    Attributes:
        batch_id (str):
        source (str):
        epoch (str):
        checkpoint (str):
        key_fields (list[str]):
        changes (list[IngestLakeChangesBodyChangesItem]):
        expected_checkpoint (None | str | Unset):
    """

    batch_id: str
    source: str
    epoch: str
    checkpoint: str
    key_fields: list[str]
    changes: list[IngestLakeChangesBodyChangesItem]
    expected_checkpoint: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        batch_id = self.batch_id

        source = self.source

        epoch = self.epoch

        checkpoint = self.checkpoint

        key_fields = self.key_fields

        changes = []
        for changes_item_data in self.changes:
            changes_item = changes_item_data.to_dict()
            changes.append(changes_item)

        expected_checkpoint: None | str | Unset
        if isinstance(self.expected_checkpoint, Unset):
            expected_checkpoint = UNSET
        else:
            expected_checkpoint = self.expected_checkpoint

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "batch_id": batch_id,
                "source": source,
                "epoch": epoch,
                "checkpoint": checkpoint,
                "key_fields": key_fields,
                "changes": changes,
            }
        )
        if expected_checkpoint is not UNSET:
            field_dict["expected_checkpoint"] = expected_checkpoint

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.ingest_lake_changes_body_changes_item import IngestLakeChangesBodyChangesItem

        d = dict(src_dict)
        batch_id = d.pop("batch_id")

        source = d.pop("source")

        epoch = d.pop("epoch")

        checkpoint = d.pop("checkpoint")

        key_fields = cast(list[str], d.pop("key_fields"))

        changes = []
        _changes = d.pop("changes")
        for changes_item_data in _changes:
            changes_item = IngestLakeChangesBodyChangesItem.from_dict(changes_item_data)

            changes.append(changes_item)

        def _parse_expected_checkpoint(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        expected_checkpoint = _parse_expected_checkpoint(d.pop("expected_checkpoint", UNSET))

        ingest_lake_changes_body = cls(
            batch_id=batch_id,
            source=source,
            epoch=epoch,
            checkpoint=checkpoint,
            key_fields=key_fields,
            changes=changes,
            expected_checkpoint=expected_checkpoint,
        )

        ingest_lake_changes_body.additional_properties = d
        return ingest_lake_changes_body

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
