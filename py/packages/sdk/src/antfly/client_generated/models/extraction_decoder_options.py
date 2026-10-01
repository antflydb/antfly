from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_decoder_options_algorithm import ExtractionDecoderOptionsAlgorithm
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionDecoderOptions")


@_attrs_define
class ExtractionDecoderOptions:
    """Bounded classification and JointIE selection. Exact optimality is with respect to admitted candidates. A completed
    beam may be feasible without an optimality proof. By default exhausted search is an error; best_effort permits only
    a validated feasible witness and reports exhausted:true.

        Attributes:
            algorithm (ExtractionDecoderOptionsAlgorithm | Unset): Omit to use the model's per-task default. GLiNER2.5 uses
                source-compatible beam selection for single-window JointIE and automatic selection for classification. Windowed
                JointIE uses the native automatic global solver with independent window resources. Explicit values select the
                native bounded search algorithm.
            beam_width (int | Unset):
            max_search_nodes (int | Unset):
            max_local_assignments (int | Unset):
            best_effort (bool | Unset):  Default: False.
    """

    algorithm: ExtractionDecoderOptionsAlgorithm | Unset = UNSET
    beam_width: int | Unset = UNSET
    max_search_nodes: int | Unset = UNSET
    max_local_assignments: int | Unset = UNSET
    best_effort: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        algorithm: str | Unset = UNSET
        if not isinstance(self.algorithm, Unset):
            algorithm = self.algorithm.value

        beam_width = self.beam_width

        max_search_nodes = self.max_search_nodes

        max_local_assignments = self.max_local_assignments

        best_effort = self.best_effort

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if algorithm is not UNSET:
            field_dict["algorithm"] = algorithm
        if beam_width is not UNSET:
            field_dict["beam_width"] = beam_width
        if max_search_nodes is not UNSET:
            field_dict["max_search_nodes"] = max_search_nodes
        if max_local_assignments is not UNSET:
            field_dict["max_local_assignments"] = max_local_assignments
        if best_effort is not UNSET:
            field_dict["best_effort"] = best_effort

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _algorithm = d.pop("algorithm", UNSET)
        algorithm: ExtractionDecoderOptionsAlgorithm | Unset
        if isinstance(_algorithm, Unset):
            algorithm = UNSET
        else:
            algorithm = ExtractionDecoderOptionsAlgorithm(_algorithm)

        beam_width = d.pop("beam_width", UNSET)

        max_search_nodes = d.pop("max_search_nodes", UNSET)

        max_local_assignments = d.pop("max_local_assignments", UNSET)

        best_effort = d.pop("best_effort", UNSET)

        extraction_decoder_options = cls(
            algorithm=algorithm,
            beam_width=beam_width,
            max_search_nodes=max_search_nodes,
            max_local_assignments=max_local_assignments,
            best_effort=best_effort,
        )

        return extraction_decoder_options
