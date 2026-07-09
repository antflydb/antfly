from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.execution_policy import ExecutionPolicy


T = TypeVar("T", bound="IndexExecutionConfig")


@_attrs_define
class IndexExecutionConfig:
    """Namespaced execution policy for index shorthand. Index configs can drive multiple operations, so each operation
    receives its own policy.

        Attributes:
            indexing (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            chunking (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            embedding (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            extracting (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            reading (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            generating (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
            transcribing (ExecutionPolicy | Unset): Non-semantic execution policy for one producer or index maintenance
                operation. These fields tune how work is batched and do not change generated artifact identity.
    """

    indexing: ExecutionPolicy | Unset = UNSET
    chunking: ExecutionPolicy | Unset = UNSET
    embedding: ExecutionPolicy | Unset = UNSET
    extracting: ExecutionPolicy | Unset = UNSET
    reading: ExecutionPolicy | Unset = UNSET
    generating: ExecutionPolicy | Unset = UNSET
    transcribing: ExecutionPolicy | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.indexing, Unset):
            indexing = self.indexing.to_dict()

        chunking: dict[str, Any] | Unset = UNSET
        if not isinstance(self.chunking, Unset):
            chunking = self.chunking.to_dict()

        embedding: dict[str, Any] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding.to_dict()

        extracting: dict[str, Any] | Unset = UNSET
        if not isinstance(self.extracting, Unset):
            extracting = self.extracting.to_dict()

        reading: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reading, Unset):
            reading = self.reading.to_dict()

        generating: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generating, Unset):
            generating = self.generating.to_dict()

        transcribing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.transcribing, Unset):
            transcribing = self.transcribing.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if indexing is not UNSET:
            field_dict["indexing"] = indexing
        if chunking is not UNSET:
            field_dict["chunking"] = chunking
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if extracting is not UNSET:
            field_dict["extracting"] = extracting
        if reading is not UNSET:
            field_dict["reading"] = reading
        if generating is not UNSET:
            field_dict["generating"] = generating
        if transcribing is not UNSET:
            field_dict["transcribing"] = transcribing

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

        _extracting = d.pop("extracting", UNSET)
        extracting: ExecutionPolicy | Unset
        if isinstance(_extracting, Unset):
            extracting = UNSET
        else:
            extracting = ExecutionPolicy.from_dict(_extracting)

        _reading = d.pop("reading", UNSET)
        reading: ExecutionPolicy | Unset
        if isinstance(_reading, Unset):
            reading = UNSET
        else:
            reading = ExecutionPolicy.from_dict(_reading)

        _generating = d.pop("generating", UNSET)
        generating: ExecutionPolicy | Unset
        if isinstance(_generating, Unset):
            generating = UNSET
        else:
            generating = ExecutionPolicy.from_dict(_generating)

        _transcribing = d.pop("transcribing", UNSET)
        transcribing: ExecutionPolicy | Unset
        if isinstance(_transcribing, Unset):
            transcribing = UNSET
        else:
            transcribing = ExecutionPolicy.from_dict(_transcribing)

        index_execution_config = cls(
            indexing=indexing,
            chunking=chunking,
            embedding=embedding,
            extracting=extracting,
            reading=reading,
            generating=generating,
            transcribing=transcribing,
        )

        index_execution_config.additional_properties = d
        return index_execution_config

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
