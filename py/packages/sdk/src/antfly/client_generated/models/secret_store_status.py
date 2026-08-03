from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="SecretStoreStatus")


@_attrs_define
class SecretStoreStatus:
    """Non-secret status for the local secrets file store, when one is available.

    Attributes:
        generation (int | Unset): Generation of the currently published secret-store snapshot.
        supports_source_generation (bool | Unset): Whether this store can expose one exact opaque source-generation
            acknowledgement. This remains true when a single loaded file predates the generation field, and is false for
            layered stores whose served snapshot has multiple publication sources.
        source_generation (None | str | Unset): Opaque, non-secret generation embedded by the control plane in the
            currently applied secrets file. It is null for files without an acknowledgement generation and never derives
            from secret values.
        last_reload_failed (bool | Unset): Whether the latest observed replacement failed to load.
        stale (bool | Unset): Whether Antfly is serving a last-known-good secrets snapshot after a failed refresh.
        reload_successes (int | Unset):
        reload_failures (int | Unset):
    """

    generation: int | Unset = UNSET
    supports_source_generation: bool | Unset = UNSET
    source_generation: None | str | Unset = UNSET
    last_reload_failed: bool | Unset = UNSET
    stale: bool | Unset = UNSET
    reload_successes: int | Unset = UNSET
    reload_failures: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation

        supports_source_generation = self.supports_source_generation

        source_generation: None | str | Unset
        if isinstance(self.source_generation, Unset):
            source_generation = UNSET
        else:
            source_generation = self.source_generation

        last_reload_failed = self.last_reload_failed

        stale = self.stale

        reload_successes = self.reload_successes

        reload_failures = self.reload_failures

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if generation is not UNSET:
            field_dict["generation"] = generation
        if supports_source_generation is not UNSET:
            field_dict["supports_source_generation"] = supports_source_generation
        if source_generation is not UNSET:
            field_dict["source_generation"] = source_generation
        if last_reload_failed is not UNSET:
            field_dict["last_reload_failed"] = last_reload_failed
        if stale is not UNSET:
            field_dict["stale"] = stale
        if reload_successes is not UNSET:
            field_dict["reload_successes"] = reload_successes
        if reload_failures is not UNSET:
            field_dict["reload_failures"] = reload_failures

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        generation = d.pop("generation", UNSET)

        supports_source_generation = d.pop("supports_source_generation", UNSET)

        def _parse_source_generation(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        source_generation = _parse_source_generation(d.pop("source_generation", UNSET))

        last_reload_failed = d.pop("last_reload_failed", UNSET)

        stale = d.pop("stale", UNSET)

        reload_successes = d.pop("reload_successes", UNSET)

        reload_failures = d.pop("reload_failures", UNSET)

        secret_store_status = cls(
            generation=generation,
            supports_source_generation=supports_source_generation,
            source_generation=source_generation,
            last_reload_failed=last_reload_failed,
            stale=stale,
            reload_successes=reload_successes,
            reload_failures=reload_failures,
        )

        secret_store_status.additional_properties = d
        return secret_store_status

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
