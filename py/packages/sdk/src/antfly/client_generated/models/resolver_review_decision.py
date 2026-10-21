from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.resolver_review_decision_decision import ResolverReviewDecisionDecision
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="ResolverReviewDecision")


@_attrs_define
class ResolverReviewDecision:
    """
    Attributes:
        decision (ResolverReviewDecisionDecision):
        actor (str | Unset):
        reason (str | Unset):
        key (str | Unset):
        table (str | Unset):
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    decision: ResolverReviewDecisionDecision
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    key: str | Unset = UNSET
    table: str | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        decision = self.decision.value

        actor = self.actor

        reason = self.reason

        key = self.key

        table = self.table

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "decision": decision,
            }
        )
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if key is not UNSET:
            field_dict["key"] = key
        if table is not UNSET:
            field_dict["table"] = table
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        decision = ResolverReviewDecisionDecision(d.pop("decision"))

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        key = d.pop("key", UNSET)

        table = d.pop("table", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        resolver_review_decision = cls(
            decision=decision,
            actor=actor,
            reason=reason,
            key=key,
            table=table,
            wait=wait,
        )

        return resolver_review_decision
