from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AgentDecision")


@_attrs_define
class AgentDecision:
    """
    Attributes:
        question_id (str): The question being answered
        answer (Union[Unset, Any]): User answer, scalar or structured depending on the question kind
        approved (Union[Unset, bool]): Used for confirm/review steps where the draft may be accepted as-is
    """

    question_id: str
    answer: Union[Unset, Any] = UNSET
    approved: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        question_id = self.question_id

        answer = self.answer

        approved = self.approved

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "question_id": question_id,
            }
        )
        if answer is not UNSET:
            field_dict["answer"] = answer
        if approved is not UNSET:
            field_dict["approved"] = approved

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        question_id = d.pop("question_id")

        answer = d.pop("answer", UNSET)

        approved = d.pop("approved", UNSET)

        agent_decision = cls(
            question_id=question_id,
            answer=answer,
            approved=approved,
        )

        agent_decision.additional_properties = d
        return agent_decision

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
