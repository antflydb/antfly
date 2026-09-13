from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_solver_status_status import ExtractionSolverStatusStatus

T = TypeVar("T", bound="ExtractionSolverStatus")


@_attrs_define
class ExtractionSolverStatus:
    """
    Attributes:
        status (ExtractionSolverStatusStatus):
        utility (float):
        visited_nodes (int):
        exhausted (bool):
    """

    status: ExtractionSolverStatusStatus
    utility: float
    visited_nodes: int
    exhausted: bool

    def to_dict(self) -> dict[str, Any]:
        status = self.status.value

        utility = self.utility

        visited_nodes = self.visited_nodes

        exhausted = self.exhausted

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "status": status,
                "utility": utility,
                "visited_nodes": visited_nodes,
                "exhausted": exhausted,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        status = ExtractionSolverStatusStatus(d.pop("status"))

        utility = d.pop("utility")

        visited_nodes = d.pop("visited_nodes")

        exhausted = d.pop("exhausted")

        extraction_solver_status = cls(
            status=status,
            utility=utility,
            visited_nodes=visited_nodes,
            exhausted=exhausted,
        )

        return extraction_solver_status
