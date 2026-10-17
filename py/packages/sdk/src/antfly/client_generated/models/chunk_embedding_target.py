from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkEmbeddingTarget")


@_attrs_define
class ChunkEmbeddingTarget:
    """
    Attributes:
        model (str):
        model_identity (str | Unset): Optional required embedding-weight/recipe identity; a changed model is rejected
            before delivery.
        task_type (str | Unset):  Default: 'RETRIEVAL_DOCUMENT'.
        dimensions (int | Unset):
        execute (bool | Unset): False resolves model limits and plans descriptors without running embedding inference.
            Default: True.
        allow_partial (bool | Unset): Return explicit per-chunk failures and gaps instead of aborting on the first non-
            retryable embedding failure. Default: False.
    """

    model: str
    model_identity: str | Unset = UNSET
    task_type: str | Unset = "RETRIEVAL_DOCUMENT"
    dimensions: int | Unset = UNSET
    execute: bool | Unset = True
    allow_partial: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        model_identity = self.model_identity

        task_type = self.task_type

        dimensions = self.dimensions

        execute = self.execute

        allow_partial = self.allow_partial

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "model": model,
            }
        )
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if execute is not UNSET:
            field_dict["execute"] = execute
        if allow_partial is not UNSET:
            field_dict["allow_partial"] = allow_partial

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        model_identity = d.pop("model_identity", UNSET)

        task_type = d.pop("task_type", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        execute = d.pop("execute", UNSET)

        allow_partial = d.pop("allow_partial", UNSET)

        chunk_embedding_target = cls(
            model=model,
            model_identity=model_identity,
            task_type=task_type,
            dimensions=dimensions,
            execute=execute,
            allow_partial=allow_partial,
        )

        chunk_embedding_target.additional_properties = d
        return chunk_embedding_target

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
