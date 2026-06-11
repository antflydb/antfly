from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_predictor_task import InferencePredictorTask
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferencePredictorInfo")


@_attrs_define
class InferencePredictorInfo:
    """Traditional ML predictor metadata.

    Attributes:
        task (InferencePredictorTask): Task type for tabular predictors.
        num_features (int): Number of feature columns expected by the predictor.
        num_outputs (int): Number of output values emitted per input row.
        feature_names (list[str] | Unset): Optional feature names in input order.
        source_framework (str | Unset): Source framework used to produce the predictor IR.
    """

    task: InferencePredictorTask
    num_features: int
    num_outputs: int
    feature_names: list[str] | Unset = UNSET
    source_framework: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        task = self.task.value

        num_features = self.num_features

        num_outputs = self.num_outputs

        feature_names: list[str] | Unset = UNSET
        if not isinstance(self.feature_names, Unset):
            feature_names = self.feature_names

        source_framework = self.source_framework

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "task": task,
                "num_features": num_features,
                "num_outputs": num_outputs,
            }
        )
        if feature_names is not UNSET:
            field_dict["feature_names"] = feature_names
        if source_framework is not UNSET:
            field_dict["source_framework"] = source_framework

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        task = InferencePredictorTask(d.pop("task"))

        num_features = d.pop("num_features")

        num_outputs = d.pop("num_outputs")

        feature_names = cast(list[str], d.pop("feature_names", UNSET))

        source_framework = d.pop("source_framework", UNSET)

        inference_predictor_info = cls(
            task=task,
            num_features=num_features,
            num_outputs=num_outputs,
            feature_names=feature_names,
            source_framework=source_framework,
        )

        inference_predictor_info.additional_properties = d
        return inference_predictor_info

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
