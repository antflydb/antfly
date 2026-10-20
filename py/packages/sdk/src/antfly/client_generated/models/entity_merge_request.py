from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_wait import ResolverWait


T = TypeVar("T", bound="EntityMergeRequest")


@_attrs_define
class EntityMergeRequest:
    """
    Attributes:
        into (str):
        replace (bool | Unset):  Default: False.
        pin_distinct (bool | Unset):  Default: False.
        wait (ResolverWait | Unset): Wait for observed replay completion up to this timeout. A timeout returns accepted
            state with applied=false; it does not undo the mutation.
    """

    into: str
    replace: bool | Unset = False
    pin_distinct: bool | Unset = False
    wait: ResolverWait | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        into = self.into

        replace = self.replace

        pin_distinct = self.pin_distinct

        wait: dict[str, Any] | Unset = UNSET
        if not isinstance(self.wait, Unset):
            wait = self.wait.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "into": into,
            }
        )
        if replace is not UNSET:
            field_dict["replace"] = replace
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if wait is not UNSET:
            field_dict["wait"] = wait

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_wait import ResolverWait

        d = dict(src_dict)
        into = d.pop("into")

        replace = d.pop("replace", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        _wait = d.pop("wait", UNSET)
        wait: ResolverWait | Unset
        if isinstance(_wait, Unset):
            wait = UNSET
        else:
            wait = ResolverWait.from_dict(_wait)

        entity_merge_request = cls(
            into=into,
            replace=replace,
            pin_distinct=pin_distinct,
            wait=wait,
        )

        return entity_merge_request
