from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalRowIndexBound")


@_attrs_define
class RelationalRowIndexBound:
    """Typed left-prefix bound in declared index order, including descending components. Inclusive bounds include the
    entire matching prefix. Integer components accept exact decimal strings; null is an indexed null.

        Attributes:
            values (list[Any]):
            inclusive (bool | Unset):  Default: True.
    """

    values: list[Any]
    inclusive: bool | Unset = True

    def to_dict(self) -> dict[str, Any]:
        values = self.values

        inclusive = self.inclusive

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "values": values,
            }
        )
        if inclusive is not UNSET:
            field_dict["inclusive"] = inclusive

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        values = cast(list[Any], d.pop("values"))

        inclusive = d.pop("inclusive", UNSET)

        relational_row_index_bound = cls(
            values=values,
            inclusive=inclusive,
        )

        return relational_row_index_bound
