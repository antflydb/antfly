from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.entity_suppress_request_absorbed import EntitySuppressRequestAbsorbed
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="EntitySuppressRequest")


@_attrs_define
class EntitySuppressRequest:
    """
    Attributes:
        absorbed (EntitySuppressRequestAbsorbed | Unset): Reject absorbed keys, release all transitive redirect sources,
            or suppress them with cascade provenance. Unsuppress never restores redirects. Default:
            EntitySuppressRequestAbsorbed.REJECT.
        pin_distinct (bool | Unset): With absorbed=release, pin each released source distinct from the target. Default:
            False.
        actor (str | Unset):
        reason (str | Unset):
        expected_revision (int | Unset):
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    absorbed: EntitySuppressRequestAbsorbed | Unset = EntitySuppressRequestAbsorbed.REJECT
    pin_distinct: bool | Unset = False
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    expected_revision: int | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        absorbed: str | Unset = UNSET
        if not isinstance(self.absorbed, Unset):
            absorbed = self.absorbed.value

        pin_distinct = self.pin_distinct

        actor = self.actor

        reason = self.reason

        expected_revision = self.expected_revision

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if absorbed is not UNSET:
            field_dict["absorbed"] = absorbed
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        _absorbed = d.pop("absorbed", UNSET)
        absorbed: EntitySuppressRequestAbsorbed | Unset
        if isinstance(_absorbed, Unset):
            absorbed = UNSET
        else:
            absorbed = EntitySuppressRequestAbsorbed(_absorbed)

        pin_distinct = d.pop("pin_distinct", UNSET)

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        expected_revision = d.pop("expected_revision", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        entity_suppress_request = cls(
            absorbed=absorbed,
            pin_distinct=pin_distinct,
            actor=actor,
            reason=reason,
            expected_revision=expected_revision,
            wait=wait,
        )

        return entity_suppress_request
