from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="TermiteRewriteRequest")


@_attrs_define
class TermiteRewriteRequest:
    """
    Attributes:
        model (str): Name of Seq2Seq rewriter model from models_dir/rewriters/ Example: lmqg/flan-t5-small-squad-qg.
        inputs (list[str]): Input texts to rewrite/transform Example: ['Translate to German: Hello, how are you?'].
    """

    model: str
    inputs: list[str]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        inputs = self.inputs

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "inputs": inputs,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        inputs = cast(list[str], d.pop("inputs"))

        termite_rewrite_request = cls(
            model=model,
            inputs=inputs,
        )

        termite_rewrite_request.additional_properties = d
        return termite_rewrite_request

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
