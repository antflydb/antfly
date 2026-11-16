from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.write_admission_config import WriteAdmissionConfig


T = TypeVar("T", bound="ResourcesConfigWrite")


@_attrs_define
class ResourcesConfigWrite:
    """
    Attributes:
        admission (WriteAdmissionConfig | Unset):
    """

    admission: WriteAdmissionConfig | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        admission: dict[str, Any] | Unset = UNSET
        if not isinstance(self.admission, Unset):
            admission = self.admission.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if admission is not UNSET:
            field_dict["admission"] = admission

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.write_admission_config import WriteAdmissionConfig

        d = dict(src_dict)
        _admission = d.pop("admission", UNSET)
        admission: WriteAdmissionConfig | Unset
        if isinstance(_admission, Unset):
            admission = UNSET
        else:
            admission = WriteAdmissionConfig.from_dict(_admission)

        resources_config_write = cls(
            admission=admission,
        )

        return resources_config_write
