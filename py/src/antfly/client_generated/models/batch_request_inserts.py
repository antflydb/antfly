from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.batch_request_inserts_additional_property import BatchRequestInsertsAdditionalProperty


T = TypeVar("T", bound="BatchRequestInserts")


@_attrs_define
class BatchRequestInserts:
    """Map of document IDs to document objects. Each key is the unique identifier for the document.

    Best practices:
    - Use consistent key naming schemes (e.g., "user:123", "article:456")
    - Key length affects storage and performance - keep them reasonably short
    - Keys are sorted lexicographically, so choose prefixes that support range scans

        Example:
            {'user:123': {'name': 'John Doe', 'email': 'john@example.com', 'age': 30, 'tags': ['customer', 'premium']},
                'user:456': {'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 25, 'tags': ['customer']}}

    """

    additional_properties: dict[str, "BatchRequestInsertsAdditionalProperty"] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.batch_request_inserts_additional_property import BatchRequestInsertsAdditionalProperty

        d = dict(src_dict)
        batch_request_inserts = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = BatchRequestInsertsAdditionalProperty.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        batch_request_inserts.additional_properties = additional_properties
        return batch_request_inserts

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> "BatchRequestInsertsAdditionalProperty":
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: "BatchRequestInsertsAdditionalProperty") -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
