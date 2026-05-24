from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_backend_capabilities import TermiteBackendCapabilities


T = TypeVar("T", bound="TermiteVersionResponse")


@_attrs_define
class TermiteVersionResponse:
    """
    Attributes:
        version (str): Termite version Example: v1.0.0.
        git_commit (str): Git commit hash Example: abc1234.
        build_time (str): Build timestamp Example: 2024-01-15T10:30:00Z.
        go_version (str): Go runtime version Example: go1.25.0.
        runtime (str | Unset): Termite runtime implementation Example: termite-zig.
        backends (TermiteBackendCapabilities | Unset):
        allow_downloads (bool | Unset): Whether model downloads are allowed in this deployment Example: True.
    """

    version: str
    git_commit: str
    build_time: str
    go_version: str
    runtime: str | Unset = UNSET
    backends: TermiteBackendCapabilities | Unset = UNSET
    allow_downloads: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        git_commit = self.git_commit

        build_time = self.build_time

        go_version = self.go_version

        runtime = self.runtime

        backends: dict[str, Any] | Unset = UNSET
        if not isinstance(self.backends, Unset):
            backends = self.backends.to_dict()

        allow_downloads = self.allow_downloads

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "version": version,
                "git_commit": git_commit,
                "build_time": build_time,
                "go_version": go_version,
            }
        )
        if runtime is not UNSET:
            field_dict["runtime"] = runtime
        if backends is not UNSET:
            field_dict["backends"] = backends
        if allow_downloads is not UNSET:
            field_dict["allow_downloads"] = allow_downloads

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.termite_backend_capabilities import TermiteBackendCapabilities

        d = dict(src_dict)
        version = d.pop("version")

        git_commit = d.pop("git_commit")

        build_time = d.pop("build_time")

        go_version = d.pop("go_version")

        runtime = d.pop("runtime", UNSET)

        _backends = d.pop("backends", UNSET)
        backends: TermiteBackendCapabilities | Unset
        if isinstance(_backends, Unset):
            backends = UNSET
        else:
            backends = TermiteBackendCapabilities.from_dict(_backends)

        allow_downloads = d.pop("allow_downloads", UNSET)

        termite_version_response = cls(
            version=version,
            git_commit=git_commit,
            build_time=build_time,
            go_version=go_version,
            runtime=runtime,
            backends=backends,
            allow_downloads=allow_downloads,
        )

        termite_version_response.additional_properties = d
        return termite_version_response

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
