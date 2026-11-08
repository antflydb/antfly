from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_policy import PrivacyPolicy
    from ..models.privacy_preview_request_sample import PrivacyPreviewRequestSample


T = TypeVar("T", bound="PrivacyPreviewRequest")


@_attrs_define
class PrivacyPreviewRequest:
    """
    Attributes:
        policy (PrivacyPolicy):
        sample (PrivacyPreviewRequestSample | Unset):
    """

    policy: PrivacyPolicy
    sample: PrivacyPreviewRequestSample | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy.to_dict()

        sample: dict[str, Any] | Unset = UNSET
        if not isinstance(self.sample, Unset):
            sample = self.sample.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "policy": policy,
            }
        )
        if sample is not UNSET:
            field_dict["sample"] = sample

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_policy import PrivacyPolicy
        from ..models.privacy_preview_request_sample import PrivacyPreviewRequestSample

        d = dict(src_dict)
        policy = PrivacyPolicy.from_dict(d.pop("policy"))

        _sample = d.pop("sample", UNSET)
        sample: PrivacyPreviewRequestSample | Unset
        if isinstance(_sample, Unset):
            sample = UNSET
        else:
            sample = PrivacyPreviewRequestSample.from_dict(_sample)

        privacy_preview_request = cls(
            policy=policy,
            sample=sample,
        )

        return privacy_preview_request
