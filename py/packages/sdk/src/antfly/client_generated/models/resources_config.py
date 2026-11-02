from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resources_config_inference import ResourcesConfigInference
    from ..models.resources_config_memory import ResourcesConfigMemory
    from ..models.resources_config_query import ResourcesConfigQuery
    from ..models.resources_config_write import ResourcesConfigWrite


T = TypeVar("T", bound="ResourcesConfig")


@_attrs_define
class ResourcesConfig:
    """Shared node-local resource policy for DB and standalone inference. All memory quantities are bytes.

    Attributes:
        memory (ResourcesConfigMemory | Unset):
        query (ResourcesConfigQuery | Unset):
        write (ResourcesConfigWrite | Unset):
        inference (ResourcesConfigInference | Unset):
    """

    memory: ResourcesConfigMemory | Unset = UNSET
    query: ResourcesConfigQuery | Unset = UNSET
    write: ResourcesConfigWrite | Unset = UNSET
    inference: ResourcesConfigInference | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        memory: dict[str, Any] | Unset = UNSET
        if not isinstance(self.memory, Unset):
            memory = self.memory.to_dict()

        query: dict[str, Any] | Unset = UNSET
        if not isinstance(self.query, Unset):
            query = self.query.to_dict()

        write: dict[str, Any] | Unset = UNSET
        if not isinstance(self.write, Unset):
            write = self.write.to_dict()

        inference: dict[str, Any] | Unset = UNSET
        if not isinstance(self.inference, Unset):
            inference = self.inference.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if memory is not UNSET:
            field_dict["memory"] = memory
        if query is not UNSET:
            field_dict["query"] = query
        if write is not UNSET:
            field_dict["write"] = write
        if inference is not UNSET:
            field_dict["inference"] = inference

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resources_config_inference import ResourcesConfigInference
        from ..models.resources_config_memory import ResourcesConfigMemory
        from ..models.resources_config_query import ResourcesConfigQuery
        from ..models.resources_config_write import ResourcesConfigWrite

        d = dict(src_dict)
        _memory = d.pop("memory", UNSET)
        memory: ResourcesConfigMemory | Unset
        if isinstance(_memory, Unset):
            memory = UNSET
        else:
            memory = ResourcesConfigMemory.from_dict(_memory)

        _query = d.pop("query", UNSET)
        query: ResourcesConfigQuery | Unset
        if isinstance(_query, Unset):
            query = UNSET
        else:
            query = ResourcesConfigQuery.from_dict(_query)

        _write = d.pop("write", UNSET)
        write: ResourcesConfigWrite | Unset
        if isinstance(_write, Unset):
            write = UNSET
        else:
            write = ResourcesConfigWrite.from_dict(_write)

        _inference = d.pop("inference", UNSET)
        inference: ResourcesConfigInference | Unset
        if isinstance(_inference, Unset):
            inference = UNSET
        else:
            inference = ResourcesConfigInference.from_dict(_inference)

        resources_config = cls(
            memory=memory,
            query=query,
            write=write,
            inference=inference,
        )

        return resources_config
