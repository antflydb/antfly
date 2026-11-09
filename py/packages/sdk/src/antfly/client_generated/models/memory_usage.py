from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_utility import MemoryUtility


T = TypeVar("T", bound="MemoryUsage")


@_attrs_define
class MemoryUsage:
    """
    Attributes:
        recalls (int | Unset):  Default: 0.
        last_recalled_at (int | None | Unset):
        last_verified_at (int | None | Unset):
        utility (MemoryUtility | Unset):
    """

    recalls: int | Unset = 0
    last_recalled_at: int | None | Unset = UNSET
    last_verified_at: int | None | Unset = UNSET
    utility: MemoryUtility | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        recalls = self.recalls

        last_recalled_at: int | None | Unset
        if isinstance(self.last_recalled_at, Unset):
            last_recalled_at = UNSET
        else:
            last_recalled_at = self.last_recalled_at

        last_verified_at: int | None | Unset
        if isinstance(self.last_verified_at, Unset):
            last_verified_at = UNSET
        else:
            last_verified_at = self.last_verified_at

        utility: dict[str, Any] | Unset = UNSET
        if not isinstance(self.utility, Unset):
            utility = self.utility.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if recalls is not UNSET:
            field_dict["recalls"] = recalls
        if last_recalled_at is not UNSET:
            field_dict["last_recalled_at"] = last_recalled_at
        if last_verified_at is not UNSET:
            field_dict["last_verified_at"] = last_verified_at
        if utility is not UNSET:
            field_dict["utility"] = utility

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_utility import MemoryUtility

        d = dict(src_dict)
        recalls = d.pop("recalls", UNSET)

        def _parse_last_recalled_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        last_recalled_at = _parse_last_recalled_at(d.pop("last_recalled_at", UNSET))

        def _parse_last_verified_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        last_verified_at = _parse_last_verified_at(d.pop("last_verified_at", UNSET))

        _utility = d.pop("utility", UNSET)
        utility: MemoryUtility | Unset
        if isinstance(_utility, Unset):
            utility = UNSET
        else:
            utility = MemoryUtility.from_dict(_utility)

        memory_usage = cls(
            recalls=recalls,
            last_recalled_at=last_recalled_at,
            last_verified_at=last_verified_at,
            utility=utility,
        )

        return memory_usage
