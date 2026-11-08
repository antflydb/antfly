from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.privacy_policy import PrivacyPolicy
    from ..models.privacy_producer import PrivacyProducer
    from ..models.privacy_status_coverage import PrivacyStatusCoverage


T = TypeVar("T", bound="PrivacyStatus")


@_attrs_define
class PrivacyStatus:
    """
    Attributes:
        configured (bool):
        policy (PrivacyPolicy | Unset):
        policy_version (str | Unset):
        resource_version (int | Unset):
        generation (int | Unset):
        scan_id (str | Unset):
        etag (str | Unset):
        resolved_producer (PrivacyProducer | Unset):
        external_processing (bool | Unset):
        coverage (PrivacyStatusCoverage | Unset):
    """

    configured: bool
    policy: PrivacyPolicy | Unset = UNSET
    policy_version: str | Unset = UNSET
    resource_version: int | Unset = UNSET
    generation: int | Unset = UNSET
    scan_id: str | Unset = UNSET
    etag: str | Unset = UNSET
    resolved_producer: PrivacyProducer | Unset = UNSET
    external_processing: bool | Unset = UNSET
    coverage: PrivacyStatusCoverage | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        configured = self.configured

        policy: dict[str, Any] | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.to_dict()

        policy_version = self.policy_version

        resource_version = self.resource_version

        generation = self.generation

        scan_id = self.scan_id

        etag = self.etag

        resolved_producer: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolved_producer, Unset):
            resolved_producer = self.resolved_producer.to_dict()

        external_processing = self.external_processing

        coverage: dict[str, Any] | Unset = UNSET
        if not isinstance(self.coverage, Unset):
            coverage = self.coverage.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "configured": configured,
            }
        )
        if policy is not UNSET:
            field_dict["policy"] = policy
        if policy_version is not UNSET:
            field_dict["policy_version"] = policy_version
        if resource_version is not UNSET:
            field_dict["resource_version"] = resource_version
        if generation is not UNSET:
            field_dict["generation"] = generation
        if scan_id is not UNSET:
            field_dict["scan_id"] = scan_id
        if etag is not UNSET:
            field_dict["etag"] = etag
        if resolved_producer is not UNSET:
            field_dict["resolved_producer"] = resolved_producer
        if external_processing is not UNSET:
            field_dict["external_processing"] = external_processing
        if coverage is not UNSET:
            field_dict["coverage"] = coverage

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.privacy_policy import PrivacyPolicy
        from ..models.privacy_producer import PrivacyProducer
        from ..models.privacy_status_coverage import PrivacyStatusCoverage

        d = dict(src_dict)
        configured = d.pop("configured")

        _policy = d.pop("policy", UNSET)
        policy: PrivacyPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = PrivacyPolicy.from_dict(_policy)

        policy_version = d.pop("policy_version", UNSET)

        resource_version = d.pop("resource_version", UNSET)

        generation = d.pop("generation", UNSET)

        scan_id = d.pop("scan_id", UNSET)

        etag = d.pop("etag", UNSET)

        _resolved_producer = d.pop("resolved_producer", UNSET)
        resolved_producer: PrivacyProducer | Unset
        if isinstance(_resolved_producer, Unset):
            resolved_producer = UNSET
        else:
            resolved_producer = PrivacyProducer.from_dict(_resolved_producer)

        external_processing = d.pop("external_processing", UNSET)

        _coverage = d.pop("coverage", UNSET)
        coverage: PrivacyStatusCoverage | Unset
        if isinstance(_coverage, Unset):
            coverage = UNSET
        else:
            coverage = PrivacyStatusCoverage.from_dict(_coverage)

        privacy_status = cls(
            configured=configured,
            policy=policy,
            policy_version=policy_version,
            resource_version=resource_version,
            generation=generation,
            scan_id=scan_id,
            etag=etag,
            resolved_producer=resolved_producer,
            external_processing=external_processing,
            coverage=coverage,
        )

        privacy_status.additional_properties = d
        return privacy_status

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
