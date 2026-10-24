from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_dataset_request_visibility import MemoryEvaluationDatasetRequestVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_case import MemoryEvaluationCase


T = TypeVar("T", bound="MemoryEvaluationDatasetRequest")


@_attrs_define
class MemoryEvaluationDatasetRequest:
    """
    Attributes:
        id (str):
        expected_revision (int):
        cases (list[MemoryEvaluationCase] | Unset):
        remove (list[str] | Unset):
        visibility (MemoryEvaluationDatasetRequestVisibility | Unset):  Default:
            MemoryEvaluationDatasetRequestVisibility.PRIVATE.
    """

    id: str
    expected_revision: int
    cases: list[MemoryEvaluationCase] | Unset = UNSET
    remove: list[str] | Unset = UNSET
    visibility: MemoryEvaluationDatasetRequestVisibility | Unset = MemoryEvaluationDatasetRequestVisibility.PRIVATE

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        cases: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.cases, Unset):
            cases = []
            for cases_item_data in self.cases:
                cases_item = cases_item_data.to_dict()
                cases.append(cases_item)

        remove: list[str] | Unset = UNSET
        if not isinstance(self.remove, Unset):
            remove = self.remove

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
            }
        )
        if cases is not UNSET:
            field_dict["cases"] = cases
        if remove is not UNSET:
            field_dict["remove"] = remove
        if visibility is not UNSET:
            field_dict["visibility"] = visibility

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_case import MemoryEvaluationCase

        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        _cases = d.pop("cases", UNSET)
        cases: list[MemoryEvaluationCase] | Unset = UNSET
        if _cases is not UNSET:
            cases = []
            for cases_item_data in _cases:
                cases_item = MemoryEvaluationCase.from_dict(cases_item_data)

                cases.append(cases_item)

        remove = cast(list[str], d.pop("remove", UNSET))

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryEvaluationDatasetRequestVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryEvaluationDatasetRequestVisibility(_visibility)

        memory_evaluation_dataset_request = cls(
            id=id,
            expected_revision=expected_revision,
            cases=cases,
            remove=remove,
            visibility=visibility,
        )

        return memory_evaluation_dataset_request
