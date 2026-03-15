from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RetryConfig")


@_attrs_define
class RetryConfig:
    """Retry configuration for generator calls

    Attributes:
        max_attempts (Union[Unset, int]): Maximum number of retry attempts Default: 3.
        initial_backoff_ms (Union[Unset, int]): Initial backoff delay in milliseconds Default: 1000.
        backoff_multiplier (Union[Unset, float]): Multiplier for exponential backoff Default: 2.0.
        max_backoff_ms (Union[Unset, int]): Maximum backoff delay in milliseconds Default: 30000.
    """

    max_attempts: Union[Unset, int] = 3
    initial_backoff_ms: Union[Unset, int] = 1000
    backoff_multiplier: Union[Unset, float] = 2.0
    max_backoff_ms: Union[Unset, int] = 30000
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        max_attempts = self.max_attempts

        initial_backoff_ms = self.initial_backoff_ms

        backoff_multiplier = self.backoff_multiplier

        max_backoff_ms = self.max_backoff_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if max_attempts is not UNSET:
            field_dict["max_attempts"] = max_attempts
        if initial_backoff_ms is not UNSET:
            field_dict["initial_backoff_ms"] = initial_backoff_ms
        if backoff_multiplier is not UNSET:
            field_dict["backoff_multiplier"] = backoff_multiplier
        if max_backoff_ms is not UNSET:
            field_dict["max_backoff_ms"] = max_backoff_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_attempts = d.pop("max_attempts", UNSET)

        initial_backoff_ms = d.pop("initial_backoff_ms", UNSET)

        backoff_multiplier = d.pop("backoff_multiplier", UNSET)

        max_backoff_ms = d.pop("max_backoff_ms", UNSET)

        retry_config = cls(
            max_attempts=max_attempts,
            initial_backoff_ms=initial_backoff_ms,
            backoff_multiplier=backoff_multiplier,
            max_backoff_ms=max_backoff_ms,
        )

        retry_config.additional_properties = d
        return retry_config

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
