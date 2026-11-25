from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentContextDocument")


@_attrs_define
class EnrichmentContextDocument:
    """
    Attributes:
        key_from (str): JSON pointer into the source document yielding the referenced key.
        table (str | Unset): Table holding the referenced document; omitted means the enrichment's own table. Resolved
            to an immutable table identity at admission, so renaming or recreating a table cannot retarget the dependency.
    """

    key_from: str
    table: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        key_from = self.key_from

        table = self.table

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "key_from": key_from,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key_from = d.pop("key_from")

        table = d.pop("table", UNSET)

        enrichment_context_document = cls(
            key_from=key_from,
            table=table,
        )

        return enrichment_context_document
