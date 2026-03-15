from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="SSEStepStarted")


@_attrs_define
class SSEStepStarted:
    """Emitted when a pipeline step begins execution

    Attributes:
        id (str): Unique step ID for correlating with step_completed Example: step_cr3ig20h5tbs73e3ahrg.
        step (str): Name of the step (e.g., "semantic_search", "tree_search") Example: semantic_search.
        action (str): Human-readable description of the action being taken Example: Searching for OAuth configuration in
            doc_embeddings index.
    """

    id: str
    step: str
    action: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        step = self.step

        action = self.action

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "step": step,
                "action": action,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        step = d.pop("step")

        action = d.pop("action")

        sse_step_started = cls(
            id=id,
            step=step,
            action=action,
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
