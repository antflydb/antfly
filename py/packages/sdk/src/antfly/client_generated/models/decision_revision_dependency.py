from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.decision_revision_dependency_kind import DecisionRevisionDependencyKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="DecisionRevisionDependency")


@_attrs_define
class DecisionRevisionDependency:
    """Flat execution dependency relative to the upstream Decisions invocation. Model entries pin generated model
    revisions; cascade entries pin definitions. Unverified entries mark incomplete historical provenance and cannot
    authorize receipt reuse.

        Attributes:
            kind (DecisionRevisionDependencyKind):
            path (list[str]):
            identity (str):
            model (str | Unset):
    """

    kind: DecisionRevisionDependencyKind
    path: list[str]
    identity: str
    model: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        path = self.path

        identity = self.identity

        model = self.model

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "path": path,
                "identity": identity,
            }
        )
        if model is not UNSET:
            field_dict["model"] = model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = DecisionRevisionDependencyKind(d.pop("kind"))

        path = cast(list[str], d.pop("path"))

        identity = d.pop("identity")

        model = d.pop("model", UNSET)

        decision_revision_dependency = cls(
            kind=kind,
            path=path,
            identity=identity,
            model=model,
        )

        return decision_revision_dependency
