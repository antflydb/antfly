from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.value_verification import ValueVerification


T = TypeVar("T", bound="QueryExecution")


@_attrs_define
class QueryExecution:
    """Execution controls; these affect admitted work, never predicate meaning or read freshness.

    Attributes:
        value_verification (ValueVerification | Unset): Execution policy for logical string-value operations. Limits
            apply to
            verification work per shard request, including repeated leaf evaluations.
            Exhaustion fails explicitly; it never returns a successful partial result.
    """

    value_verification: ValueVerification | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        value_verification: dict[str, Any] | Unset = UNSET
        if not isinstance(self.value_verification, Unset):
            value_verification = self.value_verification.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if value_verification is not UNSET:
            field_dict["value_verification"] = value_verification

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.value_verification import ValueVerification

        d = dict(src_dict)
        _value_verification = d.pop("value_verification", UNSET)
        value_verification: ValueVerification | Unset
        if isinstance(_value_verification, Unset):
            value_verification = UNSET
        else:
            value_verification = ValueVerification.from_dict(_value_verification)

        query_execution = cls(
            value_verification=value_verification,
        )

        return query_execution
