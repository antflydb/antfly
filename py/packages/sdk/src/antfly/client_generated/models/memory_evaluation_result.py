from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_prediction import MemoryEvaluationPrediction
    from ..models.memory_evaluation_result_model_type_0 import MemoryEvaluationResultModelType0


T = TypeVar("T", bound="MemoryEvaluationResult")


@_attrs_define
class MemoryEvaluationResult:
    """
    Attributes:
        run (str):
        case_id (str):
        case_revision (int):
        input_fingerprint (str):
        principal (str):
        actor (str):
        model (MemoryEvaluationResultModelType0 | None):
        revision (int):
        recorded_at (int):
        invalid_output (bool | Unset):  Default: False.
        scrubbed (bool | Unset):  Default: False.
        prediction (MemoryEvaluationPrediction | None | Unset):
        unavailable (bool | Unset):  Default: False.
    """

    run: str
    case_id: str
    case_revision: int
    input_fingerprint: str
    principal: str
    actor: str
    model: MemoryEvaluationResultModelType0 | None
    revision: int
    recorded_at: int
    invalid_output: bool | Unset = False
    scrubbed: bool | Unset = False
    prediction: MemoryEvaluationPrediction | None | Unset = UNSET
    unavailable: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_prediction import MemoryEvaluationPrediction
        from ..models.memory_evaluation_result_model_type_0 import MemoryEvaluationResultModelType0

        run = self.run

        case_id = self.case_id

        case_revision = self.case_revision

        input_fingerprint = self.input_fingerprint

        principal = self.principal

        actor = self.actor

        model: dict[str, Any] | None
        if isinstance(self.model, MemoryEvaluationResultModelType0):
            model = self.model.to_dict()
        else:
            model = self.model

        revision = self.revision

        recorded_at = self.recorded_at

        invalid_output = self.invalid_output

        scrubbed = self.scrubbed

        prediction: dict[str, Any] | None | Unset
        if isinstance(self.prediction, Unset):
            prediction = UNSET
        elif isinstance(self.prediction, MemoryEvaluationPrediction):
            prediction = self.prediction.to_dict()
        else:
            prediction = self.prediction

        unavailable = self.unavailable

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "run": run,
                "case_id": case_id,
                "case_revision": case_revision,
                "input_fingerprint": input_fingerprint,
                "principal": principal,
                "actor": actor,
                "model": model,
                "revision": revision,
                "recorded_at": recorded_at,
            }
        )
        if invalid_output is not UNSET:
            field_dict["invalid_output"] = invalid_output
        if scrubbed is not UNSET:
            field_dict["scrubbed"] = scrubbed
        if prediction is not UNSET:
            field_dict["prediction"] = prediction
        if unavailable is not UNSET:
            field_dict["unavailable"] = unavailable

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_prediction import MemoryEvaluationPrediction
        from ..models.memory_evaluation_result_model_type_0 import MemoryEvaluationResultModelType0

        d = dict(src_dict)
        run = d.pop("run")

        case_id = d.pop("case_id")

        case_revision = d.pop("case_revision")

        input_fingerprint = d.pop("input_fingerprint")

        principal = d.pop("principal")

        actor = d.pop("actor")

        def _parse_model(data: object) -> MemoryEvaluationResultModelType0 | None:
            if data is None:
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                model_type_0 = MemoryEvaluationResultModelType0.from_dict(data)

                return model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationResultModelType0 | None, data)

        model = _parse_model(d.pop("model"))

        revision = d.pop("revision")

        recorded_at = d.pop("recorded_at")

        invalid_output = d.pop("invalid_output", UNSET)

        scrubbed = d.pop("scrubbed", UNSET)

        def _parse_prediction(data: object) -> MemoryEvaluationPrediction | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                prediction_type_1 = MemoryEvaluationPrediction.from_dict(data)

                return prediction_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationPrediction | None | Unset, data)

        prediction = _parse_prediction(d.pop("prediction", UNSET))

        unavailable = d.pop("unavailable", UNSET)

        memory_evaluation_result = cls(
            run=run,
            case_id=case_id,
            case_revision=case_revision,
            input_fingerprint=input_fingerprint,
            principal=principal,
            actor=actor,
            model=model,
            revision=revision,
            recorded_at=recorded_at,
            invalid_output=invalid_output,
            scrubbed=scrubbed,
            prediction=prediction,
            unavailable=unavailable,
        )

        return memory_evaluation_result
