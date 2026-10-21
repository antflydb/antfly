from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.curation_proposal_options_absorbed import CurationProposalOptionsAbsorbed
from ..types import UNSET, Unset

T = TypeVar("T", bound="CurationProposalOptions")


@_attrs_define
class CurationProposalOptions:
    """
    Attributes:
        replace (bool | Unset):  Default: False.
        pin_distinct (bool | Unset):  Default: False.
        absorbed (CurationProposalOptionsAbsorbed | Unset):  Default: CurationProposalOptionsAbsorbed.REJECT.
    """

    replace: bool | Unset = False
    pin_distinct: bool | Unset = False
    absorbed: CurationProposalOptionsAbsorbed | Unset = CurationProposalOptionsAbsorbed.REJECT

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
        absorbed: CurationProposalOptionsAbsorbed | Unset
        if isinstance(_absorbed, Unset):
            absorbed = UNSET
        else:
            absorbed = CurationProposalOptionsAbsorbed(_absorbed)

        curation_proposal_options = cls(
            replace=replace,
            pin_distinct=pin_distinct,
            absorbed=absorbed,
        )

        return curation_proposal_options
