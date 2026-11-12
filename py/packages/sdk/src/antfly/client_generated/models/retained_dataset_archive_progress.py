from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="RetainedDatasetArchiveProgress")


@_attrs_define
class RetainedDatasetArchiveProgress:
    """
    Attributes:
        pages (str):
        sealed (bool):
        next_bucket_ns (str):
    """

    pages: str
    sealed: bool
    next_bucket_ns: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        pages = self.pages

        sealed = self.sealed

        next_bucket_ns = self.next_bucket_ns

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "pages": pages,
                "sealed": sealed,
                "next_bucket_ns": next_bucket_ns,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        pages = d.pop("pages")

        sealed = d.pop("sealed")

        next_bucket_ns = d.pop("next_bucket_ns")

        retained_dataset_archive_progress = cls(
            pages=pages,
            sealed=sealed,
            next_bucket_ns=next_bucket_ns,
        )

        retained_dataset_archive_progress.additional_properties = d
        return retained_dataset_archive_progress

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
