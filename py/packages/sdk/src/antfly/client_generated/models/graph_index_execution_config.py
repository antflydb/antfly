from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy


T = TypeVar("T", bound="GraphIndexExecutionConfig")


@_attrs_define
class GraphIndexExecutionConfig:
    """Execution policy for graph index work. Artifact producer batching belongs on graph artifact.execution.

    Attributes:
        indexing (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
            operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    indexing: ExecutionPolicy | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.indexing, Unset):
            indexing = self.indexing.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if indexing is not UNSET:
            field_dict["indexing"] = indexing

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.execution_policy import ExecutionPolicy

        d = dict(src_dict)
        _indexing = d.pop("indexing", UNSET)
        indexing: ExecutionPolicy | Unset
        if isinstance(_indexing, Unset):
            indexing = UNSET
        else:
            indexing = ExecutionPolicy.from_dict(_indexing)

        graph_index_execution_config = cls(
            indexing=indexing,
        )

        return graph_index_execution_config
