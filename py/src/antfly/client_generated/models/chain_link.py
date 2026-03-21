from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chain_condition import ChainCondition
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.generator_config import GeneratorConfig
    from ..models.retry_config import RetryConfig


T = TypeVar("T", bound="ChainLink")


@_attrs_define
class ChainLink:
    """A single link in a generator chain with optional retry and condition

    Attributes:
        generator (GeneratorConfig): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
        retry (Union[Unset, RetryConfig]): Retry configuration for generator calls
        condition (Union[Unset, ChainCondition]): Condition for trying the next generator in chain:
            - always: Always try next regardless of outcome
            - on_error: Try next on any error (default)
            - on_timeout: Try next only on timeout errors
            - on_rate_limit: Try next only on rate limit errors
    """

    generator: "GeneratorConfig"
    retry: Union[Unset, "RetryConfig"] = UNSET
    condition: Union[Unset, ChainCondition] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        generator = self.generator.to_dict()

        retry: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.retry, Unset):
            retry = self.retry.to_dict()

        condition: Union[Unset, str] = UNSET
        if not isinstance(self.condition, Unset):
            condition = self.condition.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "generator": generator,
            }
        )
        if retry is not UNSET:
            field_dict["retry"] = retry
        if condition is not UNSET:
            field_dict["condition"] = condition

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.generator_config import GeneratorConfig
        from ..models.retry_config import RetryConfig

        d = dict(src_dict)
        generator = GeneratorConfig.from_dict(d.pop("generator"))

        _retry = d.pop("retry", UNSET)
        retry: Union[Unset, RetryConfig]
        if isinstance(_retry, Unset):
            retry = UNSET
        else:
            retry = RetryConfig.from_dict(_retry)

        _condition = d.pop("condition", UNSET)
        condition: Union[Unset, ChainCondition]
        if isinstance(_condition, Unset):
            condition = UNSET
        else:
            condition = ChainCondition(_condition)

        chain_link = cls(
            generator=generator,
            retry=retry,
            condition=condition,
        )

        chain_link.additional_properties = d
        return chain_link

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
