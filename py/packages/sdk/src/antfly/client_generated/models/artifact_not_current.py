from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.artifact_not_current_error import ArtifactNotCurrentError
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.artifact_dependencies import ArtifactDependencies


T = TypeVar("T", bound="ArtifactNotCurrent")


@_attrs_define
class ArtifactNotCurrent:
    """
    Attributes:
        error (ArtifactNotCurrentError):
        message (str):
        retryable (bool):
        dependencies (ArtifactDependencies): Dependency state of one document artifact whose enrichment declares
            `context_sources`. `freshness` is null when the artifact was never
            generated with context dependencies. Freshness (`current`, `stale`,
            `failed`, `pending`) is reported separately from rebuild execution
            state. `versions` lists retained lineage manifests, newest first.
        pending (list[str] | Unset): What kept the artifact from being current when the wait ended (for example `stale`,
            `inbox`, or `outbox:<table>`).
    """

    error: ArtifactNotCurrentError
    message: str
    retryable: bool
    dependencies: ArtifactDependencies
    pending: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error.value

        message = self.message

        retryable = self.retryable

        dependencies = self.dependencies.to_dict()

        pending: list[str] | Unset = UNSET
        if not isinstance(self.pending, Unset):
            pending = self.pending

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "error": error,
                "message": message,
                "retryable": retryable,
                "dependencies": dependencies,
            }
        )
        if pending is not UNSET:
            field_dict["pending"] = pending

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.artifact_dependencies import ArtifactDependencies

        d = dict(src_dict)
        error = ArtifactNotCurrentError(d.pop("error"))

        message = d.pop("message")

        retryable = d.pop("retryable")

        dependencies = ArtifactDependencies.from_dict(d.pop("dependencies"))

        pending = cast(list[str], d.pop("pending", UNSET))

        artifact_not_current = cls(
            error=error,
            message=message,
            retryable=retryable,
            dependencies=dependencies,
            pending=pending,
        )

        artifact_not_current.additional_properties = d
        return artifact_not_current

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
