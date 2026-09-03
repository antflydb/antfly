from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_generate_batch_response_object import InferenceGenerateBatchResponseObject
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_batch_execution_report import InferenceBatchExecutionReport
    from ..models.inference_generate_batch_result_item import InferenceGenerateBatchResultItem
    from ..models.inference_generate_batch_summary import InferenceGenerateBatchSummary


T = TypeVar("T", bound="InferenceGenerateBatchResponse")


@_attrs_define
class InferenceGenerateBatchResponse:
    """
    Attributes:
        object_ (InferenceGenerateBatchResponseObject):
        data (list[InferenceGenerateBatchResultItem]):
        summary (InferenceGenerateBatchSummary):
        execution (InferenceBatchExecutionReport | Unset): Observed executor behavior, not a capability prediction.
    """

    object_: InferenceGenerateBatchResponseObject
    data: list[InferenceGenerateBatchResultItem]
    summary: InferenceGenerateBatchSummary
    execution: InferenceBatchExecutionReport | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        object_ = self.object_.value

        data = []
        for data_item_data in self.data:
            data_item = data_item_data.to_dict()
            data.append(data_item)

        summary = self.summary.to_dict()

        execution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.execution, Unset):
            execution = self.execution.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "object": object_,
                "data": data,
                "summary": summary,
            }
        )
        if execution is not UNSET:
            field_dict["execution"] = execution

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_batch_execution_report import InferenceBatchExecutionReport
        from ..models.inference_generate_batch_result_item import InferenceGenerateBatchResultItem
        from ..models.inference_generate_batch_summary import InferenceGenerateBatchSummary

        d = dict(src_dict)
        object_ = InferenceGenerateBatchResponseObject(d.pop("object"))

        data = []
        _data = d.pop("data")
        for data_item_data in _data:
            data_item = InferenceGenerateBatchResultItem.from_dict(data_item_data)

            data.append(data_item)

        summary = InferenceGenerateBatchSummary.from_dict(d.pop("summary"))

        _execution = d.pop("execution", UNSET)
        execution: InferenceBatchExecutionReport | Unset
        if isinstance(_execution, Unset):
            execution = UNSET
        else:
            execution = InferenceBatchExecutionReport.from_dict(_execution)

        inference_generate_batch_response = cls(
            object_=object_,
            data=data,
            summary=summary,
            execution=execution,
        )

        inference_generate_batch_response.additional_properties = d
        return inference_generate_batch_response

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
