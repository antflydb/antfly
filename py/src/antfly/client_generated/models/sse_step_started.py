from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_step_kind import AgentStepKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="SSEStepStarted")


@_attrs_define
class SSEStepStarted:
    """Emitted when a pipeline step begins execution

    Attributes:
        id (str): Unique step ID for correlating with step_completed Example: step_cr3ig20h5tbs73e3ahrg.
        name (str): Stable step name used for correlation across JSON and SSE surfaces
        action (str): Human-readable description of the action being taken Example: Searching for OAuth configuration in
            doc_embeddings index.
        kind (Union[Unset, AgentStepKind]): Shared bounded-agent step category
    """

    id: str
    name: str
    action: str
    kind: Union[Unset, AgentStepKind] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        action = self.action

        kind: Union[Unset, str] = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "action": action,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        action = d.pop("action")

        _kind = d.pop("kind", UNSET)
        kind: Union[Unset, AgentStepKind]
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = AgentStepKind(_kind)

        sse_step_started = cls(
            id=id,
            name=name,
            action=action,
            kind=kind,
        )

        sse_step_started.additional_properties = d
        return sse_step_started

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
