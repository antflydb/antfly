from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_entity_correction_action import MemoryEntityCorrectionAction
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEntityCorrection")


@_attrs_define
class MemoryEntityCorrection:
    """
    Attributes:
        action (MemoryEntityCorrectionAction):
        from_ (str):
        expected_revision (int):
        to (None | str | Unset):
        replace (bool | Unset):
        pin_distinct (bool | Unset):
    """

    action: MemoryEntityCorrectionAction
    from_: str
    expected_revision: int
    to: None | str | Unset = UNSET
    replace: bool | Unset = UNSET
    pin_distinct: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        action = self.action.value

        from_ = self.from_

        expected_revision = self.expected_revision

        to: None | str | Unset
        if isinstance(self.to, Unset):
            to = UNSET
        else:
            to = self.to

        replace = self.replace

        pin_distinct = self.pin_distinct

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "action": action,
                "from": from_,
                "expected_revision": expected_revision,
            }
        )
        if to is not UNSET:
            field_dict["to"] = to
        if replace is not UNSET:
            field_dict["replace"] = replace
        if pin_distinct is not UNSET:
            field_dict["pin_distinct"] = pin_distinct

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        action = MemoryEntityCorrectionAction(d.pop("action"))

        from_ = d.pop("from")

        expected_revision = d.pop("expected_revision")

        def _parse_to(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        to = _parse_to(d.pop("to", UNSET))

        replace = d.pop("replace", UNSET)

        pin_distinct = d.pop("pin_distinct", UNSET)

        memory_entity_correction = cls(
            action=action,
            from_=from_,
            expected_revision=expected_revision,
            to=to,
            replace=replace,
            pin_distinct=pin_distinct,
        )

        return memory_entity_correction
