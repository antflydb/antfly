from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="PriorArtifactContext")


@_attrs_define
class PriorArtifactContext:
    """
    Attributes:
        artifact (str): Artifact providing earlier items, including this producer's own artifact.
        render (str): Template rendered for each prior item, newest first.
        max_items (int | Unset):  Default: 40.
        max_bytes (int | Unset):  Default: 16384.
        horizon_segments (int | Unset): Earlier segments available for context and the maximum later segments
            invalidated at each affected position of an original source change. A move invalidates the old and new
            neighborhoods independently; recomputed outputs never extend either interval. Default: 8.
    """

    artifact: str
    render: str
    max_items: int | Unset = 40
    max_bytes: int | Unset = 16384
    horizon_segments: int | Unset = 8

    def to_dict(self) -> dict[str, Any]:
        artifact = self.artifact

        render = self.render

        max_items = self.max_items

        max_bytes = self.max_bytes

        horizon_segments = self.horizon_segments

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "artifact": artifact,
                "render": render,
            }
        )
        if max_items is not UNSET:
            field_dict["max_items"] = max_items
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes
        if horizon_segments is not UNSET:
            field_dict["horizon_segments"] = horizon_segments

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        artifact = d.pop("artifact")

        render = d.pop("render")

        max_items = d.pop("max_items", UNSET)

        max_bytes = d.pop("max_bytes", UNSET)

        horizon_segments = d.pop("horizon_segments", UNSET)

        prior_artifact_context = cls(
            artifact=artifact,
            render=render,
            max_items=max_items,
            max_bytes=max_bytes,
            horizon_segments=horizon_segments,
        )

        return prior_artifact_context
