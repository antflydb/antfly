from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="GraphDocumentMatchAllFilterMatchAll")


@_attrs_define
class GraphDocumentMatchAllFilterMatchAll:
    """ """

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        graph_document_match_all_filter_match_all = cls()

        return graph_document_match_all_filter_match_all
