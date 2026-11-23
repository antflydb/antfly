from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.changefeed_page_version import ChangefeedPageVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.changefeed_event import ChangefeedEvent
    from ..models.changefeed_page_snapshot import ChangefeedPageSnapshot
    from ..models.changefeed_page_transaction_type_0 import ChangefeedPageTransactionType0


T = TypeVar("T", bound="ChangefeedPage")


@_attrs_define
class ChangefeedPage:
    """
    Attributes:
        version (ChangefeedPageVersion):
        events (list[ChangefeedEvent]):
        transaction (ChangefeedPageTransactionType0 | None | Unset):
        snapshot (ChangefeedPageSnapshot | Unset):
    """

    version: ChangefeedPageVersion
    events: list[ChangefeedEvent]
    transaction: ChangefeedPageTransactionType0 | None | Unset = UNSET
    snapshot: ChangefeedPageSnapshot | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.changefeed_page_transaction_type_0 import ChangefeedPageTransactionType0

        version = self.version.value

        events = []
        for events_item_data in self.events:
            events_item = events_item_data.to_dict()
            events.append(events_item)

        transaction: dict[str, Any] | None | Unset
        if isinstance(self.transaction, Unset):
            transaction = UNSET
        elif isinstance(self.transaction, ChangefeedPageTransactionType0):
            transaction = self.transaction.to_dict()
        else:
            transaction = self.transaction

        snapshot: dict[str, Any] | Unset = UNSET
        if not isinstance(self.snapshot, Unset):
            snapshot = self.snapshot.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "version": version,
                "events": events,
            }
        )
        if transaction is not UNSET:
            field_dict["transaction"] = transaction
        if snapshot is not UNSET:
            field_dict["snapshot"] = snapshot

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.changefeed_event import ChangefeedEvent
        from ..models.changefeed_page_snapshot import ChangefeedPageSnapshot
        from ..models.changefeed_page_transaction_type_0 import ChangefeedPageTransactionType0

        d = dict(src_dict)
        version = ChangefeedPageVersion(d.pop("version"))

        events = []
        _events = d.pop("events")
        for events_item_data in _events:
            events_item = ChangefeedEvent.from_dict(events_item_data)

            events.append(events_item)

        def _parse_transaction(data: object) -> ChangefeedPageTransactionType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                transaction_type_0 = ChangefeedPageTransactionType0.from_dict(data)

                return transaction_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ChangefeedPageTransactionType0 | None | Unset, data)

        transaction = _parse_transaction(d.pop("transaction", UNSET))

        _snapshot = d.pop("snapshot", UNSET)
        snapshot: ChangefeedPageSnapshot | Unset
        if isinstance(_snapshot, Unset):
            snapshot = UNSET
        else:
            snapshot = ChangefeedPageSnapshot.from_dict(_snapshot)

        changefeed_page = cls(
            version=version,
            events=events,
            transaction=transaction,
            snapshot=snapshot,
        )

        changefeed_page.additional_properties = d
        return changefeed_page

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
