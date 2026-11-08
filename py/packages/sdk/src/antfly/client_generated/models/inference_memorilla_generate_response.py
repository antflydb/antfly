from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.inference_memorilla_generate_response_answer_path import InferenceMemorillaGenerateResponseAnswerPath
from ..models.inference_memorilla_generate_response_context_source import (
    InferenceMemorillaGenerateResponseContextSource,
)
from ..models.inference_memorilla_generate_response_finish_reason import InferenceMemorillaGenerateResponseFinishReason
from ..models.inference_memorilla_generate_response_memory_tokens import InferenceMemorillaGenerateResponseMemoryTokens
from ..models.inference_memorilla_generate_response_source_alignment import (
    InferenceMemorillaGenerateResponseSourceAlignment,
)
from ..models.inference_memorilla_generate_response_verification_status import (
    InferenceMemorillaGenerateResponseVerificationStatus,
)
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memorilla_generate_usage import MemorillaGenerateUsage


T = TypeVar("T", bound="InferenceMemorillaGenerateResponse")


@_attrs_define
class InferenceMemorillaGenerateResponse:
    """
    Attributes:
        model (str):
        text (str):
        token_ids (list[int]):
        finish_reason (InferenceMemorillaGenerateResponseFinishReason): Abstained is a policy-produced UNKNOWN answer
            with empty token_ids and zero final completion tokens. Source-verified mode never publishes a truncated factual
            answer.
        input_documents (int):
        memory_tokens (InferenceMemorillaGenerateResponseMemoryTokens): Memory slots used by the published answer.
            Sixteen for latent or approved memory answers; zero for a source-text fallback or policy abstention. Source-
            verified mode still evaluates an initial memory candidate.
        context_source (InferenceMemorillaGenerateResponseContextSource):
        encoder_execution_identity (str):
        decoder_execution_identity (str):
        memory_module_sha256 (str):
        usage (MemorillaGenerateUsage):
        answer_path (InferenceMemorillaGenerateResponseAnswerPath | Unset): Present in source-verified mode and
            identifies the actual published answer path.
        verification_status (InferenceMemorillaGenerateResponseVerificationStatus | Unset): Model-supported means a
            completed verifier emitted the exact positive support label. It is a model judgment, not a formal entailment
            proof or a guarantee that caller-provided source facts are true.
        source_alignment (InferenceMemorillaGenerateResponseSourceAlignment | Unset): Source text and embedding row
            association was supplied by the caller; native embeddings are not recomputed to certify it.
        document_text_sha256s (list[str] | Unset): Source-verified mode only. SHA256 of each original UTF-8 source text,
            preserving row order; identifies supplied bytes, not source authority or selected supporting citations.
    """

    model: str
    text: str
    token_ids: list[int]
    finish_reason: InferenceMemorillaGenerateResponseFinishReason
    input_documents: int
    memory_tokens: InferenceMemorillaGenerateResponseMemoryTokens
    context_source: InferenceMemorillaGenerateResponseContextSource
    encoder_execution_identity: str
    decoder_execution_identity: str
    memory_module_sha256: str
    usage: MemorillaGenerateUsage
    answer_path: InferenceMemorillaGenerateResponseAnswerPath | Unset = UNSET
    verification_status: InferenceMemorillaGenerateResponseVerificationStatus | Unset = UNSET
    source_alignment: InferenceMemorillaGenerateResponseSourceAlignment | Unset = UNSET
    document_text_sha256s: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        text = self.text

        token_ids = self.token_ids

        finish_reason = self.finish_reason.value

        input_documents = self.input_documents

        memory_tokens = self.memory_tokens.value

        context_source = self.context_source.value

        encoder_execution_identity = self.encoder_execution_identity

        decoder_execution_identity = self.decoder_execution_identity

        memory_module_sha256 = self.memory_module_sha256

        usage = self.usage.to_dict()

        answer_path: str | Unset = UNSET
        if not isinstance(self.answer_path, Unset):
            answer_path = self.answer_path.value

        verification_status: str | Unset = UNSET
        if not isinstance(self.verification_status, Unset):
            verification_status = self.verification_status.value

        source_alignment: str | Unset = UNSET
        if not isinstance(self.source_alignment, Unset):
            source_alignment = self.source_alignment.value

        document_text_sha256s: list[str] | Unset = UNSET
        if not isinstance(self.document_text_sha256s, Unset):
            document_text_sha256s = self.document_text_sha256s

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "text": text,
                "token_ids": token_ids,
                "finish_reason": finish_reason,
                "input_documents": input_documents,
                "memory_tokens": memory_tokens,
                "context_source": context_source,
                "encoder_execution_identity": encoder_execution_identity,
                "decoder_execution_identity": decoder_execution_identity,
                "memory_module_sha256": memory_module_sha256,
                "usage": usage,
            }
        )
        if answer_path is not UNSET:
            field_dict["answer_path"] = answer_path
        if verification_status is not UNSET:
            field_dict["verification_status"] = verification_status
        if source_alignment is not UNSET:
            field_dict["source_alignment"] = source_alignment
        if document_text_sha256s is not UNSET:
            field_dict["document_text_sha256s"] = document_text_sha256s

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memorilla_generate_usage import MemorillaGenerateUsage

        d = dict(src_dict)
        model = d.pop("model")

        text = d.pop("text")

        token_ids = cast(list[int], d.pop("token_ids"))

        finish_reason = InferenceMemorillaGenerateResponseFinishReason(d.pop("finish_reason"))

        input_documents = d.pop("input_documents")

        memory_tokens = InferenceMemorillaGenerateResponseMemoryTokens(d.pop("memory_tokens"))

        context_source = InferenceMemorillaGenerateResponseContextSource(d.pop("context_source"))

        encoder_execution_identity = d.pop("encoder_execution_identity")

        decoder_execution_identity = d.pop("decoder_execution_identity")

        memory_module_sha256 = d.pop("memory_module_sha256")

        usage = MemorillaGenerateUsage.from_dict(d.pop("usage"))

        _answer_path = d.pop("answer_path", UNSET)
        answer_path: InferenceMemorillaGenerateResponseAnswerPath | Unset
        if isinstance(_answer_path, Unset):
            answer_path = UNSET
        else:
            answer_path = InferenceMemorillaGenerateResponseAnswerPath(_answer_path)

        _verification_status = d.pop("verification_status", UNSET)
        verification_status: InferenceMemorillaGenerateResponseVerificationStatus | Unset
        if isinstance(_verification_status, Unset):
            verification_status = UNSET
        else:
            verification_status = InferenceMemorillaGenerateResponseVerificationStatus(_verification_status)

        _source_alignment = d.pop("source_alignment", UNSET)
        source_alignment: InferenceMemorillaGenerateResponseSourceAlignment | Unset
        if isinstance(_source_alignment, Unset):
            source_alignment = UNSET
        else:
            source_alignment = InferenceMemorillaGenerateResponseSourceAlignment(_source_alignment)

        document_text_sha256s = cast(list[str], d.pop("document_text_sha256s", UNSET))

        inference_memorilla_generate_response = cls(
            model=model,
            text=text,
            token_ids=token_ids,
            finish_reason=finish_reason,
            input_documents=input_documents,
            memory_tokens=memory_tokens,
            context_source=context_source,
            encoder_execution_identity=encoder_execution_identity,
            decoder_execution_identity=decoder_execution_identity,
            memory_module_sha256=memory_module_sha256,
            usage=usage,
            answer_path=answer_path,
            verification_status=verification_status,
            source_alignment=source_alignment,
            document_text_sha256s=document_text_sha256s,
        )

        return inference_memorilla_generate_response
