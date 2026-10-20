from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="ResolverMergeRequest")


@_attrs_define
class ResolverMergeRequest:
    """
    Attributes:
        from_ (str):
        to (str):
        actor (str | Unset):
        reason (str | Unset):
        replace (bool | Unset):  Default: False.
        pin_distinct (bool | Unset): Pin the replaced former target distinct. Requires an existing different redirect
            and replace=true. Default: False.
        expected_revision (int | Unset): Optional revision fence. Explicit zero fences the first edit; omission disables
            the fence.
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    from_: str
    to: str
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    replace: bool | Unset = False
    pin_distinct: bool | Unset = False
    expected_revision: int | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from_ = self.from_

        to = self.to

        actor = self.actor

        reason = self.reason

        replace = self.replace

        pin_distinct = self.pin_distinct

        expected_revision = self.expected_revision

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "from": from_,
                "to": to,
            }
        )
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if replace is not UNSET:
            field_dict["replace"] = replace
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
        from_ = d.pop("from")

        to = d.pop("to")

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        replace = d.pop("replace", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        expected_revision = d.pop("expected_revision", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        resolver_merge_request = cls(
            from_=from_,
            to=to,
            actor=actor,
            reason=reason,
            replace=replace,
            pin_distinct=pin_distinct,
            expected_revision=expected_revision,
            wait=wait,
        )

        return resolver_merge_request
