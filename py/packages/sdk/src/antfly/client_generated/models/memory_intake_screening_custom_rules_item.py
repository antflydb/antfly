from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_intake_screening_custom_rules_item_alphabet import MemoryIntakeScreeningCustomRulesItemAlphabet
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryIntakeScreeningCustomRulesItem")


@_attrs_define
class MemoryIntakeScreeningCustomRulesItem:
    """
    Attributes:
        id (str):
        prefix (str):
        min_length (int):
        max_length (int | Unset):  Default: 4096.
        alphabet (MemoryIntakeScreeningCustomRulesItemAlphabet | Unset):  Default:
            MemoryIntakeScreeningCustomRulesItemAlphabet.BASE64URL.
    """

    id: str
    prefix: str
    min_length: int
    max_length: int | Unset = 4096
    alphabet: MemoryIntakeScreeningCustomRulesItemAlphabet | Unset = (
        MemoryIntakeScreeningCustomRulesItemAlphabet.BASE64URL
    )

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        prefix = self.prefix

        min_length = self.min_length

        max_length = self.max_length

        alphabet: str | Unset = UNSET
        if not isinstance(self.alphabet, Unset):
            alphabet = self.alphabet.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "prefix": prefix,
                "min_length": min_length,
            }
        )
        if max_length is not UNSET:
            field_dict["max_length"] = max_length
        if alphabet is not UNSET:
            field_dict["alphabet"] = alphabet

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        prefix = d.pop("prefix")

        min_length = d.pop("min_length")

        max_length = d.pop("max_length", UNSET)

        _alphabet = d.pop("alphabet", UNSET)
        alphabet: MemoryIntakeScreeningCustomRulesItemAlphabet | Unset
        if isinstance(_alphabet, Unset):
            alphabet = UNSET
        else:
            alphabet = MemoryIntakeScreeningCustomRulesItemAlphabet(_alphabet)

        memory_intake_screening_custom_rules_item = cls(
            id=id,
            prefix=prefix,
            min_length=min_length,
            max_length=max_length,
            alphabet=alphabet,
        )

        return memory_intake_screening_custom_rules_item
