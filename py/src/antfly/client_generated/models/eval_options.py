from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EvalOptions")


@_attrs_define
class EvalOptions:
    """Options for evaluation behavior

    Attributes:
        k (Union[Unset, int]): K value for @K metrics (precision@k, recall@k, ndcg@k) Default: 10.
        pass_threshold (Union[Unset, float]): Score threshold for pass/fail determination Default: 0.5.
        timeout_seconds (Union[Unset, int]): Timeout for evaluation in seconds Default: 30.
    """

    k: Union[Unset, int] = 10
    pass_threshold: Union[Unset, float] = 0.5
    timeout_seconds: Union[Unset, int] = 30
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        k = self.k

        pass_threshold = self.pass_threshold

        timeout_seconds = self.timeout_seconds

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if k is not UNSET:
            field_dict["k"] = k
        if pass_threshold is not UNSET:
            field_dict["pass_threshold"] = pass_threshold
        if timeout_seconds is not UNSET:
            field_dict["timeout_seconds"] = timeout_seconds

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        k = d.pop("k", UNSET)

        pass_threshold = d.pop("pass_threshold", UNSET)

        timeout_seconds = d.pop("timeout_seconds", UNSET)

        eval_options = cls(
            k=k,
            pass_threshold=pass_threshold,
            timeout_seconds=timeout_seconds,
        )

        eval_options.additional_properties = d
        return eval_options

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
