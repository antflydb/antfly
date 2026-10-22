from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="ResolverUnmergeRequest")


@_attrs_define
class ResolverUnmergeRequest:
    """
    Attributes:
        actor (str | Unset):
        reason (str | Unset):
        pin_distinct (bool | Unset):  Default: False.
        expected_revision (int | Unset): Optional revision fence. Explicit zero fences the first edit; omission disables
            the fence.
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    pin_distinct: bool | Unset = False
    expected_revision: int | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        actor = self.actor

        reason = self.reason

        pin_distinct = self.pin_distinct

        expected_revision = self.expected_revision

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        expected_revision = d.pop("expected_revision", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        resolver_unmerge_request = cls(
            actor=actor,
            reason=reason,
            pin_distinct=pin_distinct,
            expected_revision=expected_revision,
            wait=wait,
        )

        return resolver_unmerge_request
