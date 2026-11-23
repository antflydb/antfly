from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExecutionPolicy")


@_attrs_define
class ExecutionPolicy:
    """Non-semantic execution policy for one producer or index maintenance operation. Updates persist separately from the
    frozen publication catalog and affect future invocations without invalidating accepted artifacts. To retry
    incomplete extraction or regenerate accepted output, explicitly use the bounded reprocess APIs. Node/provider
    ceilings still apply.

        Attributes:
            max_output_tokens_per_item (int | Unset): Invocation output-token budget for reader/generator assets and
                document-extraction OCR pages. Overrides producer max_tokens at execution only; it does not change the stored
                recipe, source fingerprint, or completed outputs. A lower value does not retroactively reject accepted output.
                Omit to use the producer budget.
            batch_items (int | Unset): Maximum items to process in one batch for this operation.
            batch_bytes (int | Unset): Approximate maximum source bytes to process in one batch for this operation.
            max_document_pages (int | Unset): Maximum PDF pages admitted for one request-atomic document operation.
    """

    max_output_tokens_per_item: int | Unset = UNSET
    batch_items: int | Unset = UNSET
    batch_bytes: int | Unset = UNSET
    max_document_pages: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        max_output_tokens_per_item = self.max_output_tokens_per_item

        batch_items = self.batch_items

        batch_bytes = self.batch_bytes

        max_document_pages = self.max_document_pages

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_output_tokens_per_item is not UNSET:
            field_dict["max_output_tokens_per_item"] = max_output_tokens_per_item
        if batch_items is not UNSET:
            field_dict["batch_items"] = batch_items
        if batch_bytes is not UNSET:
            field_dict["batch_bytes"] = batch_bytes
        if max_document_pages is not UNSET:
            field_dict["max_document_pages"] = max_document_pages

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_output_tokens_per_item = d.pop("max_output_tokens_per_item", UNSET)

        batch_items = d.pop("batch_items", UNSET)

        batch_bytes = d.pop("batch_bytes", UNSET)

        max_document_pages = d.pop("max_document_pages", UNSET)

        execution_policy = cls(
            max_output_tokens_per_item=max_output_tokens_per_item,
            batch_items=batch_items,
            batch_bytes=batch_bytes,
            max_document_pages=max_document_pages,
        )

        return execution_policy
