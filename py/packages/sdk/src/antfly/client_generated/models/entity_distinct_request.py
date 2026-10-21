from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="EntityDistinctRequest")


@_attrs_define
class EntityDistinctRequest:
    """
    Attributes:
        into (str):
        expected_revision (int | Unset):
        actor (str | Unset):
        reason (str | Unset):
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    into: str
    expected_revision: int | Unset = UNSET
    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        into = self.into

        expected_revision = self.expected_revision

        actor = self.actor

        reason = self.reason

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "into": into,
            }
        )
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision
        if actor is not UNSET:
            field_dict["actor"] = actor
        if reason is not UNSET:
            field_dict["reason"] = reason
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        into = d.pop("into")

        expected_revision = d.pop("expected_revision", UNSET)

        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        entity_distinct_request = cls(
            into=into,
            expected_revision=expected_revision,
            actor=actor,
            reason=reason,
            wait=wait,
        )

        return entity_distinct_request
