from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_predictor_task import InferencePredictorTask

T = TypeVar("T", bound="InferencePredictResponse")


@_attrs_define
class InferencePredictResponse:
    """
    Attributes:
        model (str):
        task (InferencePredictorTask): Task type for tabular predictors.
        predictions (list[list[float]]): Per-row prediction arrays. Length equals the model's `num_outputs`
            (1 for regression / binary, `num_classes` for multiclass).
    """

    model: str
    task: InferencePredictorTask
    predictions: list[list[float]]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        task = self.task.value

        predictions = []
        for predictions_item_data in self.predictions:
            predictions_item = predictions_item_data

            predictions.append(predictions_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "task": task,
                "predictions": predictions,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        task = InferencePredictorTask(d.pop("task"))

        predictions = []
        _predictions = d.pop("predictions")
        for predictions_item_data in _predictions:
            predictions_item = cast(list[float], predictions_item_data)

            predictions.append(predictions_item)

        inference_predict_response = cls(
            model=model,
            task=task,
            predictions=predictions,
        )

        inference_predict_response.additional_properties = d
        return inference_predict_response

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
