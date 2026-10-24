from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_dataset_visibility import MemoryEvaluationDatasetVisibility
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryEvaluationDataset")


@_attrs_define
class MemoryEvaluationDataset:
    """
    Attributes:
        id (str):
        revision (int):
        owner (str):
        branch (str):
        recorded_at (int):
        root (None | str | Unset):
        visibility (MemoryEvaluationDatasetVisibility | Unset):  Default: MemoryEvaluationDatasetVisibility.PRIVATE.
        cases (int | Unset):  Default: 0.
    """

    id: str
    revision: int
    owner: str
    branch: str
    recorded_at: int
    root: None | str | Unset = UNSET
    visibility: MemoryEvaluationDatasetVisibility | Unset = MemoryEvaluationDatasetVisibility.PRIVATE
    cases: int | Unset = 0

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        revision = self.revision

        owner = self.owner

        branch = self.branch

        recorded_at = self.recorded_at

        root: None | str | Unset
        if isinstance(self.root, Unset):
            root = UNSET
        else:
            root = self.root

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        cases = self.cases

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "revision": revision,
                "owner": owner,
                "branch": branch,
                "recorded_at": recorded_at,
            }
        )
        if root is not UNSET:
            field_dict["root"] = root
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if cases is not UNSET:
            field_dict["cases"] = cases

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        id = d.pop("id")

        revision = d.pop("revision")

        owner = d.pop("owner")

        branch = d.pop("branch")

        recorded_at = d.pop("recorded_at")

        def _parse_root(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        root = _parse_root(d.pop("root", UNSET))

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryEvaluationDatasetVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryEvaluationDatasetVisibility(_visibility)

        cases = d.pop("cases", UNSET)

        memory_evaluation_dataset = cls(
            id=id,
            revision=revision,
            owner=owner,
            branch=branch,
            recorded_at=recorded_at,
            root=root,
            visibility=visibility,
            cases=cases,
        )

        return memory_evaluation_dataset
