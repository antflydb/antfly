from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.join_operator import JoinOperator
from ..types import UNSET, Unset

T = TypeVar("T", bound="JoinCondition")


@_attrs_define
class JoinCondition:
    """Condition for matching rows between tables.

    Attributes:
        left_field (str): Field from the left (primary) table to match on. Example: customer_id.
        right_field (str): Field from the right (joined) table to match on. Example: id.
        operator (Union[Unset, JoinOperator]): Comparison operator for join condition:
            - `eq`: Equal (default)
            - `neq`: Not equal
            - `lt`: Less than
            - `lte`: Less than or equal
            - `gt`: Greater than
            - `gte`: Greater than or equal
    """

    left_field: str
    right_field: str
    operator: Union[Unset, JoinOperator] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        left_field = self.left_field

        right_field = self.right_field

        operator: Union[Unset, str] = UNSET
        if not isinstance(self.operator, Unset):
            operator = self.operator.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "left_field": left_field,
                "right_field": right_field,
            }
        )
        if operator is not UNSET:
            field_dict["operator"] = operator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        left_field = d.pop("left_field")

        right_field = d.pop("right_field")

        _operator = d.pop("operator", UNSET)
        operator: Union[Unset, JoinOperator]
        if isinstance(_operator, Unset):
            operator = UNSET
        else:
            operator = JoinOperator(_operator)

        join_condition = cls(
            left_field=left_field,
            right_field=right_field,
            operator=operator,
        )

        join_condition.additional_properties = d
        return join_condition

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
