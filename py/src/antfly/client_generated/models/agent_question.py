from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.agent_question_kind import AgentQuestionKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="AgentQuestion")


@_attrs_define
class AgentQuestion:
    """
    Attributes:
        id (str): Stable question identifier for client-carried continuation Example: clarify_oauth_version.
        kind (AgentQuestionKind): UI rendering/answer handling hint for a bounded agent question
        question (str): The clarifying question to ask the user
        reason (Union[Unset, str]): Why clarification is needed
        options (Union[Unset, list[str]]): Optional list of choices for the user
        default_answer (Union[Unset, str]): Suggested default answer when the server has a preferred choice
        affects (Union[Unset, list[str]]): High-level result areas affected by this decision
    """

    id: str
    kind: AgentQuestionKind
    question: str
    reason: Union[Unset, str] = UNSET
    options: Union[Unset, list[str]] = UNSET
    default_answer: Union[Unset, str] = UNSET
    affects: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        kind = self.kind.value

        question = self.question

        reason = self.reason

        options: Union[Unset, list[str]] = UNSET
        if not isinstance(self.options, Unset):
            options = self.options

        default_answer = self.default_answer

        affects: Union[Unset, list[str]] = UNSET
        if not isinstance(self.affects, Unset):
            affects = self.affects

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "kind": kind,
                "question": question,
            }
        )
        if reason is not UNSET:
            field_dict["reason"] = reason
        if options is not UNSET:
            field_dict["options"] = options
        if default_answer is not UNSET:
            field_dict["default_answer"] = default_answer
        if affects is not UNSET:
            field_dict["affects"] = affects

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        kind = AgentQuestionKind(d.pop("kind"))

        question = d.pop("question")

        reason = d.pop("reason", UNSET)

        options = cast(list[str], d.pop("options", UNSET))

        default_answer = d.pop("default_answer", UNSET)

        affects = cast(list[str], d.pop("affects", UNSET))

        agent_question = cls(
            id=id,
            kind=kind,
            question=question,
            reason=reason,
            options=options,
            default_answer=default_answer,
            affects=affects,
        )

        agent_question.additional_properties = d
        return agent_question

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
