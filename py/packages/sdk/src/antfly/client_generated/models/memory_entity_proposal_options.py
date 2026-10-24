from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_entity_proposal_options_absorbed import MemoryEntityProposalOptionsAbsorbed
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEntityProposalOptions")


@_attrs_define
class MemoryEntityProposalOptions:
    """
    Attributes:
        replace (bool | Unset):  Default: False.
        pin_distinct (bool | Unset):  Default: False.
        absorbed (MemoryEntityProposalOptionsAbsorbed | Unset):  Default: MemoryEntityProposalOptionsAbsorbed.REJECT.
    """

    replace: bool | Unset = False
    pin_distinct: bool | Unset = False
    absorbed: MemoryEntityProposalOptionsAbsorbed | Unset = MemoryEntityProposalOptionsAbsorbed.REJECT

    def to_dict(self) -> dict[str, Any]:
        replace = self.replace

        pin_distinct = self.pin_distinct

        absorbed: str | Unset = UNSET
        if not isinstance(self.absorbed, Unset):
            absorbed = self.absorbed.value

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if replace is not UNSET:
            field_dict["replace"] = replace
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct
        if absorbed is not UNSET:
            field_dict["absorbed"] = absorbed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        replace = d.pop("replace", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        _absorbed = d.pop("absorbed", UNSET)
        absorbed: MemoryEntityProposalOptionsAbsorbed | Unset
        if isinstance(_absorbed, Unset):
            absorbed = UNSET
        else:
            absorbed = MemoryEntityProposalOptionsAbsorbed(_absorbed)

        memory_entity_proposal_options = cls(
            replace=replace,
            pin_distinct=pin_distinct,
            absorbed=absorbed,
        )

        return memory_entity_proposal_options
