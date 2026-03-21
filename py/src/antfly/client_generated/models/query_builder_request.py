from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.generator_config import GeneratorConfig


T = TypeVar("T", bound="QueryBuilderRequest")


@_attrs_define
class QueryBuilderRequest:
    """
    Attributes:
        intent (str): Natural language description of the search intent Example: Find all published articles about
            machine learning from the last year.
        table (Union[Unset, str]): Name of the table to build query for. If provided, uses table schema for field
            context. Example: articles.
        schema_fields (Union[Unset, list[str]]): List of searchable field names to consider. Overrides table schema if
            provided. Example: ['title', 'content', 'status', 'published_at'].
        generator (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
             Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
    """

    intent: str
    table: Union[Unset, str] = UNSET
    schema_fields: Union[Unset, list[str]] = UNSET
    generator: Union[Unset, "GeneratorConfig"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        intent = self.intent

        table = self.table

        schema_fields: Union[Unset, list[str]] = UNSET
        if not isinstance(self.schema_fields, Unset):
            schema_fields = self.schema_fields

        generator: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.generator, Unset):
            generator = self.generator.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "intent": intent,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if schema_fields is not UNSET:
            field_dict["schema_fields"] = schema_fields
        if generator is not UNSET:
            field_dict["generator"] = generator

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.generator_config import GeneratorConfig

        d = dict(src_dict)
        intent = d.pop("intent")

        table = d.pop("table", UNSET)

        schema_fields = cast(list[str], d.pop("schema_fields", UNSET))

        _generator = d.pop("generator", UNSET)
        generator: Union[Unset, GeneratorConfig]
        if isinstance(_generator, Unset):
            generator = UNSET
        else:
            generator = GeneratorConfig.from_dict(_generator)

        query_builder_request = cls(
            intent=intent,
            table=table,
            schema_fields=schema_fields,
            generator=generator,
        )

        query_builder_request.additional_properties = d
        return query_builder_request

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
