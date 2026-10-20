from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryInspectionStateConfigSourceRulesItem")


@_attrs_define
class MemoryInspectionStateConfigSourceRulesItem:
    """
    Attributes:
        source_id (str):
        principal (None | str | Unset):
        producer (None | str | Unset):
        authority (int | Unset):
    """

    source_id: str
    principal: None | str | Unset = UNSET
    producer: None | str | Unset = UNSET
    authority: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        source_id = self.source_id

        principal: None | str | Unset
        if isinstance(self.principal, Unset):
            principal = UNSET
        else:
            principal = self.principal

        producer: None | str | Unset
        if isinstance(self.producer, Unset):
            producer = UNSET
        else:
            producer = self.producer

        authority = self.authority

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "source_id": source_id,
            }
        )
        if principal is not UNSET:
            field_dict["principal"] = principal
        if producer is not UNSET:
            field_dict["producer"] = producer
        if authority is not UNSET:
            field_dict["authority"] = authority

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        source_id = d.pop("source_id")

        def _parse_principal(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        principal = _parse_principal(d.pop("principal", UNSET))

        def _parse_producer(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        producer = _parse_producer(d.pop("producer", UNSET))

        authority = d.pop("authority", UNSET)

        memory_inspection_state_config_source_rules_item = cls(
            source_id=source_id,
            principal=principal,
            producer=producer,
            authority=authority,
        )

        return memory_inspection_state_config_source_rules_item
