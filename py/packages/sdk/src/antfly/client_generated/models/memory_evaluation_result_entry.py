from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_evaluation_result import MemoryEvaluationResult
    from ..models.memory_evaluation_stored_case import MemoryEvaluationStoredCase


T = TypeVar("T", bound="MemoryEvaluationResultEntry")


@_attrs_define
class MemoryEvaluationResultEntry:
    """
    Attributes:
        case (MemoryEvaluationStoredCase):
        result (MemoryEvaluationResult | None):
    """

    case: MemoryEvaluationStoredCase
    result: MemoryEvaluationResult | None

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_result import MemoryEvaluationResult

        case = self.case.to_dict()

        result: dict[str, Any] | None
        if isinstance(self.result, MemoryEvaluationResult):
            result = self.result.to_dict()
        else:
            result = self.result

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "case": case,
                "result": result,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_result import MemoryEvaluationResult
        from ..models.memory_evaluation_stored_case import MemoryEvaluationStoredCase

        d = dict(src_dict)
        case = MemoryEvaluationStoredCase.from_dict(d.pop("case"))

        def _parse_result(data: object) -> MemoryEvaluationResult | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                result_type_1 = MemoryEvaluationResult.from_dict(data)

                return result_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationResult | None, data)

        result = _parse_result(d.pop("result"))

        memory_evaluation_result_entry = cls(
            case=case,
            result=result,
        )

        return memory_evaluation_result_entry
