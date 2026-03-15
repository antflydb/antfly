from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.filter_spec_operator import FilterSpecOperator

T = TypeVar("T", bound="FilterSpec")


@_attrs_define
class FilterSpec:
    """A filter specification to apply to search queries

    Attributes:
        field (str): Field name to filter on
        operator (FilterSpecOperator): Filter operator:
            - eq: Equals
            - ne: Not equals
            - gt/gte: Greater than (or equal)
            - lt/lte: Less than (or equal)
            - contains: Contains substring
            - prefix: Starts with
            - range: Between two values (value should be array [min, max])
            - in: Value in list (value should be array)
        value (Any): Filter value (string, number, boolean, or array for range/in operators)
    """

    field: str
    operator: FilterSpecOperator
    value: Any
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        operator = self.operator.value

        value = self.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "field": field,
                "operator": operator,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        operator = FilterSpecOperator(d.pop("operator"))

        value = d.pop("value")

        filter_spec = cls(
            field=field,
            operator=operator,
            value=value,
        )

        filter_spec.additional_properties = d
        return filter_spec

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
