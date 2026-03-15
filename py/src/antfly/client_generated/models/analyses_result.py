from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="AnalysesResult")


@_attrs_define
class AnalysesResult:
    """
    Attributes:
        pca (Union[Unset, list[float]]):
        tsne (Union[Unset, list[float]]):
    """

    pca: Union[Unset, list[float]] = UNSET
    tsne: Union[Unset, list[float]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        pca: Union[Unset, list[float]] = UNSET
        if not isinstance(self.pca, Unset):
            pca = self.pca

        tsne: Union[Unset, list[float]] = UNSET
        if not isinstance(self.tsne, Unset):
            tsne = self.tsne

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if pca is not UNSET:
            field_dict["pca"] = pca
        if tsne is not UNSET:
            field_dict["tsne"] = tsne

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        pca = cast(list[float], d.pop("pca", UNSET))

        tsne = cast(list[float], d.pop("tsne", UNSET))

        analyses_result = cls(
            pca=pca,
            tsne=tsne,
        )

        analyses_result.additional_properties = d
        return analyses_result

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
