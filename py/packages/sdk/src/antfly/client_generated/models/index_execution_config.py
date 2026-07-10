from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy


T = TypeVar("T", bound="IndexExecutionConfig")


@_attrs_define
class IndexExecutionConfig:
    """Namespaced execution policy for managed index shorthand. Only namespaces with runtime effects are accepted.

    Attributes:
        chunking (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
            operation. These fields tune how work is batched and do not change generated artifact identity.
        embedding (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
            operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    chunking: ExecutionPolicy | Unset = UNSET
    embedding: ExecutionPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        chunking: dict[str, Any] | Unset = UNSET
        if not isinstance(self.chunking, Unset):
            chunking = self.chunking.to_dict()

        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if chunking is not UNSET:
            field_dict["chunking"] = chunking
        if embedding is not UNSET:
            field_dict["embedding"] = embedding

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.execution_policy import ExecutionPolicy

        d = dict(src_dict)
        _chunking = d.pop("chunking", UNSET)
        chunking: ExecutionPolicy | Unset
        if isinstance(_chunking, Unset):
            chunking = UNSET
        else:
            chunking = ExecutionPolicy.from_dict(_chunking)

        _embedding = d.pop("embedding", UNSET)
        embedding: ExecutionPolicy | Unset
        if isinstance(_embedding, Unset):
            embedding = UNSET
        else:
            embedding = ExecutionPolicy.from_dict(_embedding)

        index_execution_config = cls(
            chunking=chunking,
            embedding=embedding,
        )

        return index_execution_config
