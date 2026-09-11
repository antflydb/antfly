from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointRelation")


@_attrs_define
class ExtractionJointRelation:
    """
    Attributes:
        head (list[str]):
        tail (list[str]):
        description (str | Unset): Retained declarative metadata; model conditioning follows the pinned JointIE
            compiler.
        threshold (float | Unset): Finite centered-logit decisions require a threshold strictly between zero and one.
        candidate_threshold (float | Unset):
        directed (bool | Unset):  Default: True.
        symmetric (bool | Unset):  Default: False.
        inverse (str | Unset):
        allow_self (bool | Unset):  Default: False.
        max_per_head (int | Unset):
        max_per_tail (int | Unset):
    """

    head: list[str]
    tail: list[str]
    description: str | Unset = UNSET
    threshold: float | Unset = UNSET
    candidate_threshold: float | Unset = UNSET
    directed: bool | Unset = True
    symmetric: bool | Unset = False
    inverse: str | Unset = UNSET
    allow_self: bool | Unset = False
    max_per_head: int | Unset = UNSET
    max_per_tail: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        head = self.head

        tail = self.tail

        description = self.description

        threshold = self.threshold

        candidate_threshold = self.candidate_threshold

        directed = self.directed

        symmetric = self.symmetric

        inverse = self.inverse

        allow_self = self.allow_self

        max_per_head = self.max_per_head

        max_per_tail = self.max_per_tail

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "head": head,
                "tail": tail,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if threshold is not UNSET:
            field_dict["threshold"] = threshold
        if candidate_threshold is not UNSET:
            field_dict["candidate_threshold"] = candidate_threshold
        if directed is not UNSET:
            field_dict["directed"] = directed
        if symmetric is not UNSET:
            field_dict["symmetric"] = symmetric
        if inverse is not UNSET:
            field_dict["inverse"] = inverse
        if allow_self is not UNSET:
            field_dict["allow_self"] = allow_self
        if max_per_head is not UNSET:
            field_dict["max_per_head"] = max_per_head
        if max_per_tail is not UNSET:
            field_dict["max_per_tail"] = max_per_tail

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        head = cast(list[str], d.pop("head"))

        tail = cast(list[str], d.pop("tail"))

        description = d.pop("description", UNSET)

        threshold = d.pop("threshold", UNSET)

        candidate_threshold = d.pop("candidate_threshold", UNSET)

        directed = d.pop("directed", UNSET)

        symmetric = d.pop("symmetric", UNSET)

        inverse = d.pop("inverse", UNSET)

        allow_self = d.pop("allow_self", UNSET)

        max_per_head = d.pop("max_per_head", UNSET)

        max_per_tail = d.pop("max_per_tail", UNSET)

        extraction_joint_relation = cls(
            head=head,
            tail=tail,
            description=description,
            threshold=threshold,
            candidate_threshold=candidate_threshold,
            directed=directed,
            symmetric=symmetric,
            inverse=inverse,
            allow_self=allow_self,
            max_per_head=max_per_head,
            max_per_tail=max_per_tail,
        )

        return extraction_joint_relation
