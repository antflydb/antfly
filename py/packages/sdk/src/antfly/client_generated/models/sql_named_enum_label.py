from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="SQLNamedEnumLabel")


@_attrs_define
class SQLNamedEnumLabel:
    """Immutable enum symbol. Symbol IDs are append-only; order keys are opaque stable ordering metadata.

    Attributes:
        id (int):
        text (str): Exact UTF-8 label; PostgreSQL limits its encoded length to 63 bytes.
        order_key (str): Server-managed immutable ordering key encoded as lowercase hexadecimal.
    """

    id: int
    text: str
    order_key: str

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        text = self.text

        order_key = self.order_key

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "text": text,
                "order_key": order_key,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        text = d.pop("text")

        order_key = d.pop("order_key")

        sql_named_enum_label = cls(
            id=id,
            text=text,
            order_key=order_key,
        )

        return sql_named_enum_label
