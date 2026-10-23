from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_context_checkpoint_version import InferenceContextCheckpointVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_context_structured_memory import InferenceContextStructuredMemory


T = TypeVar("T", bound="InferenceContextCheckpoint")


@_attrs_define
class InferenceContextCheckpoint:
    """
    Attributes:
        version (InferenceContextCheckpointVersion): Structured-memory checkpoint wire version.
        covered_messages (int): Number of authoritative leading messages represented by this checkpoint.
        covered_prefix_sha256 (str): SHA-256 of the canonical covered message prefix.
        memory_sha256 (str): SHA-256 of the canonical structured memory payload.
        model_revision (str):
        tokenizer_revision (str):
        template_revision (str):
        memory (InferenceContextStructuredMemory):
        parent_checkpoint_sha256 (str | Unset):
    """

    version: InferenceContextCheckpointVersion
    covered_messages: int
    covered_prefix_sha256: str
    memory_sha256: str
    model_revision: str
    tokenizer_revision: str
    template_revision: str
    memory: InferenceContextStructuredMemory
    parent_checkpoint_sha256: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        version = self.version.value

        covered_messages = self.covered_messages

        covered_prefix_sha256 = self.covered_prefix_sha256

        memory_sha256 = self.memory_sha256

        model_revision = self.model_revision

        tokenizer_revision = self.tokenizer_revision

        template_revision = self.template_revision

        memory = self.memory.to_dict()

        parent_checkpoint_sha256 = self.parent_checkpoint_sha256

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "version": version,
                "covered_messages": covered_messages,
                "covered_prefix_sha256": covered_prefix_sha256,
                "memory_sha256": memory_sha256,
                "model_revision": model_revision,
                "tokenizer_revision": tokenizer_revision,
                "template_revision": template_revision,
                "memory": memory,
            }
        )
        if parent_checkpoint_sha256 is not UNSET:
            field_dict["parent_checkpoint_sha256"] = parent_checkpoint_sha256

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_context_structured_memory import InferenceContextStructuredMemory

        d = dict(src_dict)
        version = InferenceContextCheckpointVersion(d.pop("version"))

        covered_messages = d.pop("covered_messages")

        covered_prefix_sha256 = d.pop("covered_prefix_sha256")

        memory_sha256 = d.pop("memory_sha256")

        model_revision = d.pop("model_revision")

        tokenizer_revision = d.pop("tokenizer_revision")

        template_revision = d.pop("template_revision")

        memory = InferenceContextStructuredMemory.from_dict(d.pop("memory"))

        parent_checkpoint_sha256 = d.pop("parent_checkpoint_sha256", UNSET)

        inference_context_checkpoint = cls(
            version=version,
            covered_messages=covered_messages,
            covered_prefix_sha256=covered_prefix_sha256,
            memory_sha256=memory_sha256,
            model_revision=model_revision,
            tokenizer_revision=tokenizer_revision,
            template_revision=template_revision,
            memory=memory,
            parent_checkpoint_sha256=parent_checkpoint_sha256,
        )

        return inference_context_checkpoint
