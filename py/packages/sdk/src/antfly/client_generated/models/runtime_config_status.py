from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RuntimeConfigStatus")


@_attrs_define
class RuntimeConfigStatus:
    """Non-secret status for the applied config.json snapshot. Hot publication accepts validated remote_content-only
    changes; startup-only changes remain stale until restart.

        Attributes:
            generation (int | Unset): Generation of the fully validated and atomically published configuration.
            hash_ (str | Unset): Lowercase SHA-256 of the exact fully applied config.json bytes; its first 16 characters
                match the operator config-hash annotation.
            last_reload_failed (bool | Unset): Whether the latest observed replacement failed loading, semantic validation,
                or requires restart because startup-only fields changed.
            stale (bool | Unset): Whether requests are using the last-known-good snapshot after a failed reload.
            reload_successes (int | Unset):
            reload_failures (int | Unset):
    """

    generation: int | Unset = UNSET
    hash_: str | Unset = UNSET
    last_reload_failed: bool | Unset = UNSET
    stale: bool | Unset = UNSET
    reload_successes: int | Unset = UNSET
    reload_failures: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generation = self.generation

        hash_ = self.hash_

        last_reload_failed = self.last_reload_failed

        stale = self.stale

        reload_successes = self.reload_successes

        reload_failures = self.reload_failures

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if generation is not UNSET:
            field_dict["generation"] = generation
        if hash_ is not UNSET:
            field_dict["hash"] = hash_
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

        hash_ = d.pop("hash", UNSET)

        last_reload_failed = d.pop("last_reload_failed", UNSET)

        stale = d.pop("stale", UNSET)

        reload_successes = d.pop("reload_successes", UNSET)

        reload_failures = d.pop("reload_failures", UNSET)

        runtime_config_status = cls(
            generation=generation,
            hash_=hash_,
            last_reload_failed=last_reload_failed,
            stale=stale,
            reload_successes=reload_successes,
            reload_failures=reload_failures,
        )

        runtime_config_status.additional_properties = d
        return runtime_config_status

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
