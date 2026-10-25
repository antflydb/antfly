from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_assertion import MemoryAssertion


T = TypeVar("T", bound="MemoryAssertionRequest")


@_attrs_define
class MemoryAssertionRequest:
    """
    Attributes:
        assertions (list[MemoryAssertion]):
    """

    assertions: list[MemoryAssertion]

    def to_dict(self) -> dict[str, Any]:
        assertions = []
        for assertions_item_data in self.assertions:
            assertions_item = assertions_item_data.to_dict()
            assertions.append(assertions_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "assertions": assertions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_assertion import MemoryAssertion

        d = dict(src_dict)
        assertions = []
        _assertions = d.pop("assertions")
        for assertions_item_data in _assertions:
            assertions_item = MemoryAssertion.from_dict(assertions_item_data)

            assertions.append(assertions_item)

        memory_assertion_request = cls(
            assertions=assertions,
        )

        return memory_assertion_request
