from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_resolver_config import TermiteResolverConfig


T = TypeVar("T", bound="TermiteRecognizeRequest")


@_attrs_define
class TermiteRecognizeRequest:
    """
    Attributes:
        model (str): Name of recognizer model from models_dir/recognizers/ Example: dslim/bert-base-NER.
        texts (list[str]): Texts to extract entities from Example: ['John Smith works at Google.', 'Apple Inc. is in
            Cupertino.'].
        labels (list[str] | Unset): Custom entity labels to extract (GLiNER models only).
            When using a GLiNER model, you can specify any entity types to extract,
            enabling zero-shot NER without model retraining.
            If not provided, the model's default labels are used.
             Example: ['person', 'company', 'product', 'date'].
        relation_labels (list[str] | Unset): Relation types to extract (for models with 'relations' capability).
            Only used when the model supports relation extraction (GLiNER multitask, REBEL).
            Relation extraction runs only when this array is provided and non-empty.
            GLiNER labels may be relation names (works_for), head-qualified
            labels (person::works_for), or head/tail-qualified labels
            (person::works_for::organization).
             Example: ['founded', 'works_at', 'located_in'].
        resolver (TermiteResolverConfig | Unset): Configuration for entity resolution. When present in a
            RecognizeRequest,
            the response entities and relations are deduplicated via entity resolution
            (e.g., "Elon Musk" and "Musk" are merged into a single entity).
    """

    model: str
    texts: list[str]
    labels: list[str] | Unset = UNSET
    relation_labels: list[str] | Unset = UNSET
    resolver: TermiteResolverConfig | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        texts = self.texts

        labels: list[str] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = self.labels

        relation_labels: list[str] | Unset = UNSET
        if not isinstance(self.relation_labels, Unset):
            relation_labels = self.relation_labels

        resolver: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolver, Unset):
            resolver = self.resolver.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
                "texts": texts,
            }
        )
        if labels is not UNSET:
            field_dict["labels"] = labels
        if relation_labels is not UNSET:
            field_dict["relation_labels"] = relation_labels
        if resolver is not UNSET:
            field_dict["resolver"] = resolver

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_resolver_config import TermiteResolverConfig

        d = dict(src_dict)
        model = d.pop("model")

        texts = cast(list[str], d.pop("texts"))

        labels = cast(list[str], d.pop("labels", UNSET))

        relation_labels = cast(list[str], d.pop("relation_labels", UNSET))

        _resolver = d.pop("resolver", UNSET)
        resolver: TermiteResolverConfig | Unset
        if isinstance(_resolver, Unset):
            resolver = UNSET
        else:
            resolver = TermiteResolverConfig.from_dict(_resolver)

        termite_recognize_request = cls(
            model=model,
            texts=texts,
            labels=labels,
            relation_labels=relation_labels,
            resolver=resolver,
        )

        termite_recognize_request.additional_properties = d
        return termite_recognize_request

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
