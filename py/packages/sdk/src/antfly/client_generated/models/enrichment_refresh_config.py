from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.enrichment_refresh_config_mode import EnrichmentRefreshConfigMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentRefreshConfig")


@_attrs_define
class EnrichmentRefreshConfig:
    """Rebuild policy for context-source invalidation. Invalidation (stale freshness) is always reported immediately;
    debounce only coalesces rebuild work.

        Attributes:
            mode (EnrichmentRefreshConfigMode | Unset): `background` journals rebuilds automatically; `manual` only marks
                dependents stale (rebuild with the reprocess endpoints). Default: EnrichmentRefreshConfigMode.BACKGROUND.
            debounce (str | Unset): Coalescing delay before a background rebuild (for example `2s`).
    """

    mode: EnrichmentRefreshConfigMode | Unset = EnrichmentRefreshConfigMode.BACKGROUND
    debounce: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        debounce = self.debounce

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if debounce is not UNSET:
            field_dict["debounce"] = debounce

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: EnrichmentRefreshConfigMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = EnrichmentRefreshConfigMode(_mode)

        debounce = d.pop("debounce", UNSET)

        enrichment_refresh_config = cls(
            mode=mode,
            debounce=debounce,
        )

        return enrichment_refresh_config
