from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorillaProfileConfig")


@_attrs_define
class MemorillaProfileConfig:
    """An administrator-configured, immutable Memorilla model composition. Only the qualified Qwen3 F32 CUDA profiles are
    supported.

        Attributes:
            name (str):
            encoder (str): Managed encoder reference using the qwen3_encoder_f32_v1 execution profile.
            decoder (str): Managed decoder reference using the qwen3_decoder_f32_kv_f32_v1 execution profile.
            memory_module_path (str): Local exported released-checkpoint bundle directory. This path is configured by the
                administrator and cannot be supplied by an inference request.
            max_documents (int | Unset):  Default: 64.
            max_new_tokens (int | Unset):  Default: 128.
    """

    name: str
    encoder: str
    decoder: str
    memory_module_path: str
    max_documents: int | Unset = 64
    max_new_tokens: int | Unset = 128

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        encoder = self.encoder

        decoder = self.decoder

        memory_module_path = self.memory_module_path

        max_documents = self.max_documents

        max_new_tokens = self.max_new_tokens

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "encoder": encoder,
                "decoder": decoder,
                "memory_module_path": memory_module_path,
            }
        )
        if max_documents is not UNSET:
            field_dict["max_documents"] = max_documents
        if max_new_tokens is not UNSET:
            field_dict["max_new_tokens"] = max_new_tokens

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        encoder = d.pop("encoder")

        decoder = d.pop("decoder")

        memory_module_path = d.pop("memory_module_path")

        max_documents = d.pop("max_documents", UNSET)

        max_new_tokens = d.pop("max_new_tokens", UNSET)

        memorilla_profile_config = cls(
            name=name,
            encoder=encoder,
            decoder=decoder,
            memory_module_path=memory_module_path,
            max_documents=max_documents,
            max_new_tokens=max_new_tokens,
        )

        return memorilla_profile_config
