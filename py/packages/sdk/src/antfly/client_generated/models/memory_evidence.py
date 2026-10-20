from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evidence_visibility import MemoryEvidenceVisibility
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvidence")


@_attrs_define
class MemoryEvidence:
    """
    Attributes:
        id (str):
        source_id (str):
        source_version (str):
        text (str):
        source_url (None | str | Unset):
        author (None | str | Unset):
        event_time (int | None | Unset):
        timezone (None | str | Unset):
        thread_id (None | str | Unset):
        turn_id (None | str | Unset):
        ordinal (int | None | Unset):
        producer (str | Unset):
        managed (bool | Unset):
        visibility (MemoryEvidenceVisibility | Unset):
        expires_at (int | None | Unset):
    """

    id: str
    source_id: str
    source_version: str
    text: str
    source_url: None | str | Unset = UNSET
    author: None | str | Unset = UNSET
    event_time: int | None | Unset = UNSET
    timezone: None | str | Unset = UNSET
    thread_id: None | str | Unset = UNSET
    turn_id: None | str | Unset = UNSET
    ordinal: int | None | Unset = UNSET
    producer: str | Unset = UNSET
    managed: bool | Unset = UNSET
    visibility: MemoryEvidenceVisibility | Unset = UNSET
    expires_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        source_id = self.source_id

        source_version = self.source_version

        text = self.text

        source_url: None | str | Unset
        if isinstance(self.source_url, Unset):
            source_url = UNSET
        else:
            source_url = self.source_url

        author: None | str | Unset
        if isinstance(self.author, Unset):
            author = UNSET
        else:
            author = self.author

        event_time: int | None | Unset
        if isinstance(self.event_time, Unset):
            event_time = UNSET
        else:
            event_time = self.event_time

        timezone: None | str | Unset
        if isinstance(self.timezone, Unset):
            timezone = UNSET
        else:
            timezone = self.timezone

        thread_id: None | str | Unset
        if isinstance(self.thread_id, Unset):
            thread_id = UNSET
        else:
            thread_id = self.thread_id

        turn_id: None | str | Unset
        if isinstance(self.turn_id, Unset):
            turn_id = UNSET
        else:
            turn_id = self.turn_id

        ordinal: int | None | Unset
        if isinstance(self.ordinal, Unset):
            ordinal = UNSET
        else:
            ordinal = self.ordinal

        producer = self.producer

        managed = self.managed

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        expires_at: int | None | Unset
        if isinstance(self.expires_at, Unset):
            expires_at = UNSET
        else:
            expires_at = self.expires_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "source_id": source_id,
                "source_version": source_version,
                "text": text,
            }
        )
        if source_url is not UNSET:
            field_dict["source_url"] = source_url
        if author is not UNSET:
            field_dict["author"] = author
        if event_time is not UNSET:
            field_dict["event_time"] = event_time
        if timezone is not UNSET:
            field_dict["timezone"] = timezone
        if thread_id is not UNSET:
            field_dict["thread_id"] = thread_id
        if turn_id is not UNSET:
            field_dict["turn_id"] = turn_id
        if ordinal is not UNSET:
            field_dict["ordinal"] = ordinal
        if producer is not UNSET:
            field_dict["producer"] = producer
        if managed is not UNSET:
            field_dict["managed"] = managed
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if expires_at is not UNSET:
            field_dict["expires_at"] = expires_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        source_id = d.pop("source_id")

        source_version = d.pop("source_version")

        text = d.pop("text")

        def _parse_source_url(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        source_url = _parse_source_url(d.pop("source_url", UNSET))

        def _parse_author(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        author = _parse_author(d.pop("author", UNSET))

        def _parse_event_time(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        event_time = _parse_event_time(d.pop("event_time", UNSET))

        def _parse_timezone(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        timezone = _parse_timezone(d.pop("timezone", UNSET))

        def _parse_thread_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        thread_id = _parse_thread_id(d.pop("thread_id", UNSET))

        def _parse_turn_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        turn_id = _parse_turn_id(d.pop("turn_id", UNSET))

        def _parse_ordinal(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        ordinal = _parse_ordinal(d.pop("ordinal", UNSET))

        producer = d.pop("producer", UNSET)

        managed = d.pop("managed", UNSET)

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryEvidenceVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryEvidenceVisibility(_visibility)

        def _parse_expires_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        expires_at = _parse_expires_at(d.pop("expires_at", UNSET))

        memory_evidence = cls(
            id=id,
            source_id=source_id,
            source_version=source_version,
            text=text,
            source_url=source_url,
            author=author,
            event_time=event_time,
            timezone=timezone,
            thread_id=thread_id,
            turn_id=turn_id,
            ordinal=ordinal,
            producer=producer,
            managed=managed,
            visibility=visibility,
            expires_at=expires_at,
        )

        return memory_evidence
