from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_predictors_response_object import InferencePredictorsResponseObject

if TYPE_CHECKING:
    from ..models.inference_predictors_response_predictors import InferencePredictorsResponsePredictors


T = TypeVar("T", bound="InferencePredictorsResponse")


@_attrs_define
class InferencePredictorsResponse:
    """
    Attributes:
        object_ (InferencePredictorsResponseObject): Response object type.
        predictors (InferencePredictorsResponsePredictors): Traditional ML predictors keyed by predictor name.
    """

    object_: InferencePredictorsResponseObject
    predictors: InferencePredictorsResponsePredictors
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        predictors = self.predictors.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "predictors": predictors,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_predictors_response_predictors import InferencePredictorsResponsePredictors

        d = dict(src_dict)
        object_ = InferencePredictorsResponseObject(d.pop("object"))

        predictors = InferencePredictorsResponsePredictors.from_dict(d.pop("predictors"))

        inference_predictors_response = cls(
            object_=object_,
            predictors=predictors,
        )

        inference_predictors_response.additional_properties = d
        return inference_predictors_response

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
