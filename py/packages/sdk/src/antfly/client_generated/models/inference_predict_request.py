from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferencePredictRequest")


@_attrs_define
class InferencePredictRequest:
    """
    Attributes:
        model (str): Predictor name from the model catalog.
        input_ (list[list[float]]): Batch of feature vectors. Max 10000 rows.
    """

    model: str
    input_: list[list[float]]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        input_ = []
        for input_item_data in self.input_:
            input_item = input_item_data

            input_.append(input_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "input": input_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        input_ = []
        _input_ = d.pop("input")
        for input_item_data in _input_:
            input_item = cast(list[float], input_item_data)

            input_.append(input_item)

        inference_predict_request = cls(
            model=model,
            input_=input_,
        )

        inference_predict_request.additional_properties = d
        return inference_predict_request

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
