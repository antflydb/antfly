from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_supersession_candidate_request_mode import MemorySupersessionCandidateRequestMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorySupersessionCandidateRequest")


@_attrs_define
class MemorySupersessionCandidateRequest:
    """
    Attributes:
        max_bytes (int | Unset):  Default: 262144.
        limit (int | Unset):  Default: 32.
        mode (MemorySupersessionCandidateRequestMode | Unset):  Default:
            MemorySupersessionCandidateRequestMode.UNION_SEARCH.
        lexical_threshold (float | Unset):  Default: 0.1.
        semantic_threshold (float | Unset):  Default: 0.8.
    """

    max_bytes: int | Unset = 262144
    limit: int | Unset = 32
    mode: MemorySupersessionCandidateRequestMode | Unset = MemorySupersessionCandidateRequestMode.UNION_SEARCH
    lexical_threshold: float | Unset = 0.1
    semantic_threshold: float | Unset = 0.8

    def to_dict(self) -> dict[str, Any]:
        max_bytes = self.max_bytes

        limit = self.limit

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        lexical_threshold = self.lexical_threshold

        semantic_threshold = self.semantic_threshold

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if limit is not UNSET:
            field_dict["limit"] = limit
        if mode is not UNSET:
            field_dict["mode"] = mode
        if lexical_threshold is not UNSET:
            field_dict["lexical_threshold"] = lexical_threshold
        if semantic_threshold is not UNSET:
            field_dict["semantic_threshold"] = semantic_threshold

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_bytes = d.pop("max_bytes", UNSET)

        limit = d.pop("limit", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: MemorySupersessionCandidateRequestMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = MemorySupersessionCandidateRequestMode(_mode)

        lexical_threshold = d.pop("lexical_threshold", UNSET)

        semantic_threshold = d.pop("semantic_threshold", UNSET)

        memory_supersession_candidate_request = cls(
            max_bytes=max_bytes,
            limit=limit,
            mode=mode,
            lexical_threshold=lexical_threshold,
            semantic_threshold=semantic_threshold,
        )

        return memory_supersession_candidate_request
