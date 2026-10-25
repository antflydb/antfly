from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_kv_cache_policy_mode import InferenceKvCachePolicyMode
from ..models.inference_kv_cache_policy_version import InferenceKvCachePolicyVersion
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceKvCachePolicy")


@_attrs_define
class InferenceKvCachePolicy:
    """
    Attributes:
        mode (InferenceKvCachePolicyMode): Requested KV retention policy. `exact_hybrid` uses only model-native exact
            local/global attention retention. `select_v1` requests the versioned lossy
            selector and is rejected when the loaded model/backend has not qualified it.
        version (InferenceKvCachePolicyVersion | Unset): Wire-contract version. The initial policies use `v1` only.
            Default: InferenceKvCachePolicyVersion.V1.
    """

    mode: InferenceKvCachePolicyMode
    version: InferenceKvCachePolicyVersion | Unset = InferenceKvCachePolicyVersion.V1

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        version: str | Unset = UNSET
        if not isinstance(self.version, Unset):
            version = self.version.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "mode": mode,
            }
        )
        if version is not UNSET:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mode = InferenceKvCachePolicyMode(d.pop("mode"))

        _version = d.pop("version", UNSET)
        version: InferenceKvCachePolicyVersion | Unset
        if isinstance(_version, Unset):
            version = UNSET
        else:
            version = InferenceKvCachePolicyVersion(_version)

        inference_kv_cache_policy = cls(
            mode=mode,
            version=version,
        )

        return inference_kv_cache_policy
