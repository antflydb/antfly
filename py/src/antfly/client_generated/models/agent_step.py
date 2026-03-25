from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_step_kind import AgentStepKind
from ..models.agent_step_status import AgentStepStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.agent_step_details import AgentStepDetails


T = TypeVar("T", bound="AgentStep")


@_attrs_define
class AgentStep:
    """A step in the shared bounded-agent execution trace

    Attributes:
        name (str): Stable step name used for correlation across JSON and SSE surfaces
        action (str): What action was taken
        id (Union[Unset, str]): Unique step ID for correlation and tracing
        kind (Union[Unset, AgentStepKind]): Shared bounded-agent step category
        status (Union[Unset, AgentStepStatus]): Outcome of a shared agent step
        error_message (Union[Unset, str]): Error details when status is "error"
        duration_ms (Union[Unset, int]): Server-side execution time in milliseconds
        details (Union[Unset, AgentStepDetails]): Additional details about the step
    """

    name: str
    action: str
    id: Union[Unset, str] = UNSET
    kind: Union[Unset, AgentStepKind] = UNSET
    status: Union[Unset, AgentStepStatus] = UNSET
    error_message: Union[Unset, str] = UNSET
    duration_ms: Union[Unset, int] = UNSET
    details: Union[Unset, "AgentStepDetails"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        action = self.action

        id = self.id

        kind: Union[Unset, str] = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

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
                "name": name,
                "action": action,
            }
        )
        if id is not UNSET:
            field_dict["id"] = id
        if kind is not UNSET:
            field_dict["kind"] = kind
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
        from ..models.agent_step_details import AgentStepDetails

        d = dict(src_dict)
        name = d.pop("name")

        action = d.pop("action")

        id = d.pop("id", UNSET)

        _kind = d.pop("kind", UNSET)
        kind: Union[Unset, AgentStepKind]
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = AgentStepKind(_kind)

        _status = d.pop("status", UNSET)
        status: Union[Unset, AgentStepStatus]
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = AgentStepStatus(_status)

        error_message = d.pop("error_message", UNSET)

        duration_ms = d.pop("duration_ms", UNSET)

        _details = d.pop("details", UNSET)
        details: Union[Unset, AgentStepDetails]
        if isinstance(_details, Unset):
            details = UNSET
        else:
            details = AgentStepDetails.from_dict(_details)

        agent_step = cls(
            name=name,
            action=action,
            id=id,
            kind=kind,
            status=status,
            error_message=error_message,
            duration_ms=duration_ms,
            details=details,
        )

        agent_step.additional_properties = d
        return agent_step

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
