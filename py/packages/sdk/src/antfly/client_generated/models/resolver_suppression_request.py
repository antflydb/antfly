from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_suppression_selection import ResolverSuppressionSelection
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="ResolverSuppressionRequest")


@_attrs_define
class ResolverSuppressionRequest:
    """Add keys or normalized texts. The add/remove form updates both sets in one revision. Runtime removals do not
    override declarative suppression.

        Attributes:
            actor (str | Unset):
            reason (str | Unset):
            keys (list[str] | Unset):
            texts (list[str] | Unset):
            add (ResolverSuppressionSelection | Unset):
            remove (ResolverSuppressionSelection | Unset):
            expected_revision (int | Unset): Optional revision fence. Explicit zero fences the first edit; omission disables
                the fence.
            wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
                state with applied=false; it does not undo the mutation.
    """

    actor: str | Unset = UNSET
    reason: str | Unset = UNSET
    keys: list[str] | Unset = UNSET
    texts: list[str] | Unset = UNSET
    add: ResolverSuppressionSelection | Unset = UNSET
    remove: ResolverSuppressionSelection | Unset = UNSET
    expected_revision: int | Unset = UNSET
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        actor = self.actor

        reason = self.reason

        keys: list[str] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = self.keys

        texts: list[str] | Unset = UNSET
        if not isinstance(self.texts, Unset):
            texts = self.texts

        add: dict[str, Any] | Unset = UNSET
        if not isinstance(self.add, Unset):
            add = self.add.to_dict()

        remove: dict[str, Any] | Unset = UNSET
        if not isinstance(self.remove, Unset):
            remove = self.remove.to_dict()

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
        if keys is not UNSET:
            field_dict["keys"] = keys
        if texts is not UNSET:
            field_dict["texts"] = texts
        if add is not UNSET:
            field_dict["add"] = add
        if remove is not UNSET:
            field_dict["remove"] = remove
        if expected_revision is not UNSET:
            field_dict["expected_revision"] = expected_revision
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_suppression_selection import ResolverSuppressionSelection
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        actor = d.pop("actor", UNSET)

        reason = d.pop("reason", UNSET)

        keys = cast(list[str], d.pop("keys", UNSET))

        texts = cast(list[str], d.pop("texts", UNSET))

        _add = d.pop("add", UNSET)
        add: ResolverSuppressionSelection | Unset
        if isinstance(_add, Unset):
            add = UNSET
        else:
            add = ResolverSuppressionSelection.from_dict(_add)

        _remove = d.pop("remove", UNSET)
        remove: ResolverSuppressionSelection | Unset
        if isinstance(_remove, Unset):
            remove = UNSET
        else:
            remove = ResolverSuppressionSelection.from_dict(_remove)

        expected_revision = d.pop("expected_revision", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        resolver_suppression_request = cls(
            actor=actor,
            reason=reason,
            keys=keys,
            texts=texts,
            add=add,
            remove=remove,
            expected_revision=expected_revision,
            wait=wait,
        )

        return resolver_suppression_request
