from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_inspection_state_pending_config_type_0_intake_screening_mode import (
    MemoryInspectionStatePendingConfigType0IntakeScreeningMode,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_inspection_state_pending_config_type_0_intake_screening_custom_rules_item import (
        MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem,
    )


T = TypeVar("T", bound="MemoryInspectionStatePendingConfigType0IntakeScreening")


@_attrs_define
class MemoryInspectionStatePendingConfigType0IntakeScreening:
    """
    Attributes:
        version (int | Unset):  Default: 1.
        mode (MemoryInspectionStatePendingConfigType0IntakeScreeningMode | Unset):  Default:
            MemoryInspectionStatePendingConfigType0IntakeScreeningMode.REJECT.
        custom_rules (list[MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem] | Unset):
        allow_fingerprints (list[str] | Unset):
    """

    version: int | Unset = 1
    mode: MemoryInspectionStatePendingConfigType0IntakeScreeningMode | Unset = (
        MemoryInspectionStatePendingConfigType0IntakeScreeningMode.REJECT
    )
    custom_rules: list[MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem] | Unset = UNSET
    allow_fingerprints: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        custom_rules: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.custom_rules, Unset):
            custom_rules = []
            for custom_rules_item_data in self.custom_rules:
                custom_rules_item = custom_rules_item_data.to_dict()
                custom_rules.append(custom_rules_item)

        allow_fingerprints: list[str] | Unset = UNSET
        if not isinstance(self.allow_fingerprints, Unset):
            allow_fingerprints = self.allow_fingerprints

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if mode is not UNSET:
            field_dict["mode"] = mode
        if custom_rules is not UNSET:
            field_dict["custom_rules"] = custom_rules
        if allow_fingerprints is not UNSET:
            field_dict["allow_fingerprints"] = allow_fingerprints

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_inspection_state_pending_config_type_0_intake_screening_custom_rules_item import (
            MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem,
        )

        d = dict(src_dict)
        version = d.pop("version", UNSET)

        _mode = d.pop("mode", UNSET)
        mode: MemoryInspectionStatePendingConfigType0IntakeScreeningMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = MemoryInspectionStatePendingConfigType0IntakeScreeningMode(_mode)

        _custom_rules = d.pop("custom_rules", UNSET)
        custom_rules: list[MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem] | Unset = UNSET
        if _custom_rules is not UNSET:
            custom_rules = []
            for custom_rules_item_data in _custom_rules:
                custom_rules_item = MemoryInspectionStatePendingConfigType0IntakeScreeningCustomRulesItem.from_dict(
                    custom_rules_item_data
                )

                custom_rules.append(custom_rules_item)

        allow_fingerprints = cast(list[str], d.pop("allow_fingerprints", UNSET))

        memory_inspection_state_pending_config_type_0_intake_screening = cls(
            version=version,
            mode=mode,
            custom_rules=custom_rules,
            allow_fingerprints=allow_fingerprints,
        )

        return memory_inspection_state_pending_config_type_0_intake_screening
