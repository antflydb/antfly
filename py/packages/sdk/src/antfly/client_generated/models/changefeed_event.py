from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.changefeed_event_kind import ChangefeedEventKind
from ..models.changefeed_event_operation import ChangefeedEventOperation
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.changefeed_event_after_type_0 import ChangefeedEventAfterType0
    from ..models.changefeed_event_before_type_0 import ChangefeedEventBeforeType0
    from ..models.changefeed_key import ChangefeedKey


T = TypeVar("T", bound="ChangefeedEvent")


@_attrs_define
class ChangefeedEvent:
    """
    Attributes:
        event_id (str):
        table_id (str):
        transaction_id (str | Unset):
        version (str | Unset):
        operation (ChangefeedEventOperation | Unset):
        kind (ChangefeedEventKind | Unset):
        key (ChangefeedKey | Unset):
        before (ChangefeedEventBeforeType0 | None | Unset):
        after (ChangefeedEventAfterType0 | None | Unset):
    """

    event_id: str
    table_id: str
    transaction_id: str | Unset = UNSET
    version: str | Unset = UNSET
    operation: ChangefeedEventOperation | Unset = UNSET
    kind: ChangefeedEventKind | Unset = UNSET
    key: ChangefeedKey | Unset = UNSET
    before: ChangefeedEventBeforeType0 | None | Unset = UNSET
    after: ChangefeedEventAfterType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.changefeed_event_after_type_0 import ChangefeedEventAfterType0
        from ..models.changefeed_event_before_type_0 import ChangefeedEventBeforeType0

        event_id = self.event_id

        table_id = self.table_id

        transaction_id = self.transaction_id

        version = self.version

        operation: str | Unset = UNSET
        if not isinstance(self.operation, Unset):
            operation = self.operation.value

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        key: dict[str, Any] | Unset = UNSET
        if not isinstance(self.key, Unset):
            key = self.key.to_dict()

        before: dict[str, Any] | None | Unset
        if isinstance(self.before, Unset):
            before = UNSET
        elif isinstance(self.before, ChangefeedEventBeforeType0):
            before = self.before.to_dict()
        else:
            before = self.before

        after: dict[str, Any] | None | Unset
        if isinstance(self.after, Unset):
            after = UNSET
        elif isinstance(self.after, ChangefeedEventAfterType0):
            after = self.after.to_dict()
        else:
            after = self.after

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "event_id": event_id,
                "table_id": table_id,
            }
        )
        if transaction_id is not UNSET:
            field_dict["transaction_id"] = transaction_id
        if version is not UNSET:
            field_dict["version"] = version
        if operation is not UNSET:
            field_dict["operation"] = operation
        if kind is not UNSET:
            field_dict["kind"] = kind
        if key is not UNSET:
            field_dict["key"] = key
        if before is not UNSET:
            field_dict["before"] = before
        if after is not UNSET:
            field_dict["after"] = after

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.changefeed_event_after_type_0 import ChangefeedEventAfterType0
        from ..models.changefeed_event_before_type_0 import ChangefeedEventBeforeType0
        from ..models.changefeed_key import ChangefeedKey

        d = dict(src_dict)
        event_id = d.pop("event_id")

        table_id = d.pop("table_id")

        transaction_id = d.pop("transaction_id", UNSET)

        version = d.pop("version", UNSET)

        _operation = d.pop("operation", UNSET)
        operation: ChangefeedEventOperation | Unset
        if isinstance(_operation, Unset):
            operation = UNSET
        else:
            operation = ChangefeedEventOperation(_operation)

        _kind = d.pop("kind", UNSET)
        kind: ChangefeedEventKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ChangefeedEventKind(_kind)

        _key = d.pop("key", UNSET)
        key: ChangefeedKey | Unset
        if isinstance(_key, Unset):
            key = UNSET
        else:
            key = ChangefeedKey.from_dict(_key)

        def _parse_before(data: object) -> ChangefeedEventBeforeType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                before_type_0 = ChangefeedEventBeforeType0.from_dict(data)

                return before_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ChangefeedEventBeforeType0 | None | Unset, data)

        before = _parse_before(d.pop("before", UNSET))

        def _parse_after(data: object) -> ChangefeedEventAfterType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                after_type_0 = ChangefeedEventAfterType0.from_dict(data)

                return after_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ChangefeedEventAfterType0 | None | Unset, data)

        after = _parse_after(d.pop("after", UNSET))

        changefeed_event = cls(
            event_id=event_id,
            table_id=table_id,
            transaction_id=transaction_id,
            version=version,
            operation=operation,
            kind=kind,
            key=key,
            before=before,
            after=after,
        )

        changefeed_event.additional_properties = d
        return changefeed_event

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
