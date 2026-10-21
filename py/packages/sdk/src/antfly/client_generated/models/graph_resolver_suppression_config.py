from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphResolverSuppressionConfig")


@_attrs_define
class GraphResolverSuppressionConfig:
    """Suppress mentions before candidate search, embedding, adjudication and promotion. Suppressed mentions remain
    explainable in resolution artifacts and contribute no canonical graph edges. Changes use the normal resolver
    generation and bounded backfill lifecycle. Redirects to suppressed keys are rejected. Runtime suppressions are
    additive and outlive index versions.

        Attributes:
            keys (list[str] | Unset): Exact rendered canonical keys in this resolver namespace.
            texts (list[str] | Unset): Mention texts normalized with the resolver Unicode slug rules.
            min_text_chars (int | Unset): Minimum Unicode scalar count after trimming surrounding ASCII whitespace. Default:
                0.
    """

    keys: list[str] | Unset = UNSET
    texts: list[str] | Unset = UNSET
    min_text_chars: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        texts: list[str] | Unset = UNSET
        if not isinstance(self.texts, Unset):
            texts = self.texts

        min_text_chars = self.min_text_chars

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if keys is not UNSET:
            field_dict["keys"] = keys
        if texts is not UNSET:
            field_dict["texts"] = texts
        if min_text_chars is not UNSET:
            field_dict["min_text_chars"] = min_text_chars

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        keys = cast(list[str], d.pop("keys", UNSET))

        texts = cast(list[str], d.pop("texts", UNSET))

        min_text_chars = d.pop("min_text_chars", UNSET)

        graph_resolver_suppression_config = cls(
            keys=keys,
            texts=texts,
            min_text_chars=min_text_chars,
        )

        return graph_resolver_suppression_config
