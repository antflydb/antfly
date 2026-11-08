from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memorilla_generate_request_context_mode import MemorillaGenerateRequestContextMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="MemorillaGenerateRequest")


@_attrs_define
class MemorillaGenerateRequest:
    """Generate a buffered answer over caller-supplied stored document embeddings.
    Features must be full-width normalized outputs of the configured encoder
    execution profile. The server applies the pinned FP16-storage/BF16-readout
    conversion. The identity pins compatibility; it does not authenticate the
    origin, authorization, or collection coverage of caller-supplied vectors.
    The first release supports greedy decoding with fresh F32 KV only.
    context_mode=source_verified generates an independent source answer and
    checks its support. A memory candidate requires exact agreement with that
    supported answer; otherwise the source answer or abstention is returned.
    This explicit mode does not establish
    source truth, authorization, or text/vector association cryptographically.

        Attributes:
            model (str): Name from inference.memorilla.profiles. Requests cannot override the bound model assets or memory
                module.
            question (str): A nonempty question admitted to the configured encoder's 256-token limit after canonical
                whitespace normalization.
            document_embeddings (list[list[float]]):
            encoder_execution_identity (str): Exact model_identity returned by the qualified encoder. Ordinary mixed-
                precision encoder identities are incompatible.
            max_new_tokens (int | Unset): Maximum tokens for each answer candidate. Source-verified mode generates a memory
                candidate and an independent source answer, plus at most one verifier call of eight tokens; usage reports all
                phases. Every phase also respects profile and operator ceilings. Default: 64.
            context_mode (MemorillaGenerateRequestContextMode | Unset): Latent uses stored features alone. Source-verified
                mode requires document_texts and explicitly checks or replaces the memory candidate using their contents.
                Default: MemorillaGenerateRequestContextMode.LATENT.
            document_texts (list[str] | Unset): Required only for source_verified and rejected in latent mode. One nonblank
                UTF-8 source text per embedding row, in the same order; at most 8192 bytes each and 32768 source bytes total.
                The complete rendered prompt must fit the 1024-token internal bound and operator limits, without truncation.
                Pairing is asserted by the caller, not recomputed or authenticated.
    """

    model: str
    question: str
    document_embeddings: list[list[float]]
    encoder_execution_identity: str
    max_new_tokens: int | Unset = 64
    context_mode: MemorillaGenerateRequestContextMode | Unset = MemorillaGenerateRequestContextMode.LATENT
    document_texts: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        question = self.question

        document_embeddings = []
        for document_embeddings_item_data in self.document_embeddings:
            document_embeddings_item = document_embeddings_item_data

            document_embeddings.append(document_embeddings_item)

        encoder_execution_identity = self.encoder_execution_identity

        max_new_tokens = self.max_new_tokens

        context_mode: str | Unset = UNSET
        if not isinstance(self.context_mode, Unset):
            context_mode = self.context_mode.value

        document_texts: list[str] | Unset = UNSET
        if not isinstance(self.document_texts, Unset):
            document_texts = self.document_texts

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "question": question,
                "document_embeddings": document_embeddings,
                "encoder_execution_identity": encoder_execution_identity,
            }
        )
        if max_new_tokens is not UNSET:
            field_dict["max_new_tokens"] = max_new_tokens
        if context_mode is not UNSET:
            field_dict["context_mode"] = context_mode
        if document_texts is not UNSET:
            field_dict["document_texts"] = document_texts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        question = d.pop("question")

        document_embeddings = []
        _document_embeddings = d.pop("document_embeddings")
        for document_embeddings_item_data in _document_embeddings:
            document_embeddings_item = cast(list[float], document_embeddings_item_data)

            document_embeddings.append(document_embeddings_item)

        encoder_execution_identity = d.pop("encoder_execution_identity")

        max_new_tokens = d.pop("max_new_tokens", UNSET)

        _context_mode = d.pop("context_mode", UNSET)
        context_mode: MemorillaGenerateRequestContextMode | Unset
        if isinstance(_context_mode, Unset):
            context_mode = UNSET
        else:
            context_mode = MemorillaGenerateRequestContextMode(_context_mode)

        document_texts = cast(list[str], d.pop("document_texts", UNSET))

        memorilla_generate_request = cls(
            model=model,
            question=question,
            document_embeddings=document_embeddings,
            encoder_execution_identity=encoder_execution_identity,
            max_new_tokens=max_new_tokens,
            context_mode=context_mode,
            document_texts=document_texts,
        )

        return memorilla_generate_request
