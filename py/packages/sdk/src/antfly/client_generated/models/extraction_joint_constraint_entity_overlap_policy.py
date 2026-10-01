from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_entity_overlap_policy_policy import (
    ExtractionJointConstraintEntityOverlapPolicyPolicy,
)
from ..models.extraction_joint_constraint_entity_overlap_policy_type import (
    ExtractionJointConstraintEntityOverlapPolicyType,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintEntityOverlapPolicy")


@_attrs_define
class ExtractionJointConstraintEntityOverlapPolicy:
    """
    Attributes:
        type_ (ExtractionJointConstraintEntityOverlapPolicyType):
        policy (ExtractionJointConstraintEntityOverlapPolicyPolicy | Unset):
    """

    type_: ExtractionJointConstraintEntityOverlapPolicyType
    policy: ExtractionJointConstraintEntityOverlapPolicyPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        policy: str | Unset = UNSET
        if not isinstance(self.policy, Unset):
            policy = self.policy.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if policy is not UNSET:
            field_dict["policy"] = policy

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintEntityOverlapPolicyType(d.pop("type"))

        _policy = d.pop("policy", UNSET)
        policy: ExtractionJointConstraintEntityOverlapPolicyPolicy | Unset
        if isinstance(_policy, Unset):
            policy = UNSET
        else:
            policy = ExtractionJointConstraintEntityOverlapPolicyPolicy(_policy)

        extraction_joint_constraint_entity_overlap_policy = cls(
            type_=type_,
            policy=policy,
        )

        return extraction_joint_constraint_entity_overlap_policy
