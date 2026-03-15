from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retrieval_reasoning_step_status import RetrievalReasoningStepStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.retrieval_reasoning_step_details import RetrievalReasoningStepDetails


T = TypeVar("T", bound="RetrievalReasoningStep")


@_attrs_define
class RetrievalReasoningStep:
    """A step in the retrieval reasoning chain

    Attributes:
        step (str): Name of the tool call or action taken Example: semantic_search.
        action (str): What action was taken Example: Searched for OAuth configuration in doc_embeddings index.
        id (Union[Unset, str]): Unique step ID for correlation and tracing Example: step_cr3ig20h5tbs73e3ahrg.
        status (Union[Unset, RetrievalReasoningStepStatus]): Outcome of this step
        error_message (Union[Unset, str]): Error details when status is "error"
        duration_ms (Union[Unset, int]): Server-side execution time in milliseconds
        details (Union[Unset, RetrievalReasoningStepDetails]): Additional details about the step (e.g., tool arguments,
            result count)
    """

    step: str
    action: str
    id: Union[Unset, str] = UNSET
    status: Union[Unset, RetrievalReasoningStepStatus] = UNSET
    error_message: Union[Unset, str] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    details: Union[Unset, "RetrievalReasoningStepDetails"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        step = self.step

        action = self.action

        id = self.id

        status: Union[Unset, str] = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        error_message = self.error_message

        duration_ms = self.duration_ms

        details: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.details, Unset):
            details = self.details.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "step": step,
                "action": action,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if status is not UNSET:
            field_dict["status"] = status
        if error_message is not UNSET:
            field_dict["error_message"] = error_message
        if duration_ms is not UNSET:
            field_dict["duration_ms"] = duration_ms
        if details is not UNSET:
            field_dict["details"] = details

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retrieval_reasoning_step_details import RetrievalReasoningStepDetails

        d = dict(src_dict)
        step = d.pop("step")

        action = d.pop("action")

        id = d.pop("id", UNSET)

        _status = d.pop("status", UNSET)
        status: Union[Unset, RetrievalReasoningStepStatus]
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = RetrievalReasoningStepStatus(_status)

        error_message = d.pop("error_message", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        _details = d.pop("details", UNSET)
        details: Union[Unset, RetrievalReasoningStepDetails]
        if isinstance(_details, Unset):
            details = UNSET
        else:
            details = RetrievalReasoningStepDetails.from_dict(_details)

        retrieval_reasoning_step = cls(
            step=step,
            action=action,
            id=id,
            status=status,
            error_message=error_message,
            duration_ms=duration_ms,
            details=details,
        )

        retrieval_reasoning_step.additional_properties = d
        return retrieval_reasoning_step

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
