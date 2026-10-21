from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResolverWait")


@_attrs_define
class ResolverWait:
    """Wait for observed replay completion up to this timeout. A timeout returns accepted state with applied=false; it does
    not undo the mutation.

        Attributes:
            timeout_ms (int | Unset):  Default: 1000.
    """

    timeout_ms: int | Unset = 1000

    def to_dict(self) -> dict[str, Any]:
        timeout_ms = self.timeout_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if timeout_ms is not UNSET:
            field_dict["timeout_ms"] = timeout_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        timeout_ms = d.pop("timeout_ms", UNSET)

        resolver_wait = cls(
            timeout_ms=timeout_ms,
        )

        return resolver_wait
