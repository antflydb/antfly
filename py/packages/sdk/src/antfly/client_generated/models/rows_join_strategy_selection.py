from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.rows_join_strategy import RowsJoinStrategy

T = TypeVar("T", bound="RowsJoinStrategySelection")


@_attrs_define
class RowsJoinStrategySelection:
    """Join strategy admission metadata returned by native join execution.

    Attributes:
        requested (RowsJoinStrategy): Physical join strategy requested by the typed plan. `auto` lets Antfly choose from
            proven local/routed capabilities; `merge` requires both join inputs to be proven ordered by the leading join
            keys.
        selected (RowsJoinStrategy): Physical join strategy requested by the typed plan. `auto` lets Antfly choose from
            proven local/routed capabilities; `merge` requires both join inputs to be proven ordered by the leading join
            keys.
    """

    requested: RowsJoinStrategy
    selected: RowsJoinStrategy

    def to_dict(self) -> dict[str, Any]:
        requested = self.requested.value

        selected = self.selected.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "requested": requested,
                "selected": selected,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        requested = RowsJoinStrategy(d.pop("requested"))

        selected = RowsJoinStrategy(d.pop("selected"))

        rows_join_strategy_selection = cls(
            requested=requested,
            selected=selected,
        )

        return rows_join_strategy_selection
