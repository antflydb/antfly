from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memorilla_generate_response_answer_guard import MemorillaGenerateResponseAnswerGuard
from ..models.memorilla_generate_response_answer_path import MemorillaGenerateResponseAnswerPath
from ..models.memorilla_generate_response_context_source import MemorillaGenerateResponseContextSource
from ..models.memorilla_generate_response_finish_reason import MemorillaGenerateResponseFinishReason
from ..models.memorilla_generate_response_memory_tokens import MemorillaGenerateResponseMemoryTokens
from ..models.memorilla_generate_response_record_gate import MemorillaGenerateResponseRecordGate
from ..models.memorilla_generate_response_source_alignment import MemorillaGenerateResponseSourceAlignment
from ..models.memorilla_generate_response_verification_status import MemorillaGenerateResponseVerificationStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memorilla_citation import MemorillaCitation
    from ..models.memorilla_diagnostics import MemorillaDiagnostics
    from ..models.memorilla_generate_usage import MemorillaGenerateUsage


T = TypeVar("T", bound="MemorillaGenerateResponse")


@_attrs_define
class MemorillaGenerateResponse:
    """
    Attributes:
        model (str):
        text (str):
        token_ids (list[int]):
        finish_reason (MemorillaGenerateResponseFinishReason): Abstained is a policy-produced UNKNOWN answer. In source-
            verified mode it has empty token_ids and zero final completion tokens, and that mode never publishes a truncated
            factual answer. On encoder_lora_research and hybrid_research profiles it is produced by the answer guard (see
            answer_guard) and token_ids keep the decoder's output.
        input_documents (int):
        memory_tokens (MemorillaGenerateResponseMemoryTokens): Memory slots used by the published answer. Sixteen for
            latent or approved memory answers; zero for a source-text fallback or policy abstention. Source-verified mode
            still evaluates an initial memory candidate. hybrid_research reports 32 when the record gate opened (16 soft
            rows then 16 record rows) and 16 when it closed (soft rows only); span-window bundles report 16 plus the
            evidence window rows when it opened (48 for a 32-row window, 80 for the cascade's 64-row wide window) and 16
            when it closed or read_mode is none.
        context_source (MemorillaGenerateResponseContextSource): supplied_documents is reported by encoder_lora_research
            and hybrid_research profiles, which encoded caller-supplied texts server-side.
        encoder_execution_identity (str):
        decoder_execution_identity (str):
        memory_module_sha256 (str):
        usage (MemorillaGenerateUsage):
        candidate_sha256 (str | Unset): encoder_lora_research and hybrid_research profiles only. SHA256 of the research
            candidate.json pinned by the reader bundle and, for encoder_lora_research, by the merged encoder lock
            (hybrid_research instead checks that the candidate names the bundle's encoder-lora.safetensors digest); it
            identifies the experimental candidate, not a qualification status.
        answer_guard (MemorillaGenerateResponseAnswerGuard | Unset): encoder_lora_research and hybrid_research profiles
            only. Outcome of the answer guard applied to the decoder text. The research task answers are short exact facts
            (a name of a few words, a short code, or the literal UNKNOWN); text that is empty, longer than 64 bytes, more
            than six words, contains control characters, the memory frame literal Memory as a standalone word or the
            <|memory|> marker, or bytes outside letters, digits, space, hyphen, period and apostrophe is rejected.
            hybrid_research answers general questions in sentences, so it applies only the frame-echo rules (empty text,
            control characters including newlines, the standalone word Memory, the <|memory|> marker). A rejected answer is
            published as text UNKNOWN with finish_reason abstained; token_ids keep what the decoder produced.
        citations (list[MemorillaCitation] | Unset): encoder_lora_research and hybrid_research profiles only. Where
            every span the copy head copied into the 16-slot memory record came from, one entry per contiguous cited region
            of a document, sorted by document_index then start. A copied run that crosses from one document into the next is
            split per document, and regions of one document that touch or overlap are merged. Empty when nothing was copied
            or when citations_unavailable is true, and on hybrid_research when record_gate is closed (the decoder then read
            only soft rows, which are not traceable to source bytes). A citation shows what the memory record copied, not
            proof that the answer is correct; supports_answer only reports whether the published answer text (on
            hybrid_research, after canonicalization) occurs in the cited region.
        citations_unavailable (bool | Unset): encoder_lora_research and hybrid_research profiles only. True when the
            encoder tokenizer's pieces did not reproduce the supplied texts byte for byte (for example Unicode normalization
            changed them), so copied tokens could not be mapped back to byte ranges; citations is then empty and the answer
            is unaffected.
        record_gate (MemorillaGenerateResponseRecordGate | Unset): hybrid_research profiles only. Open when at least one
            span the copy head copied into the record snapped every slot at cosine 0.85 or more; the decoder then read 32
            rows under the merged prompt. Closed (no copied span, or every span carries a low-cosine slot) means the decoder
            read the 16 soft rows alone under the released helpful prompt.
        canonicalized (bool | Unset): hybrid_research profiles only. True when the published text was re-spelled from
            the record (record casing for a word-aligned case-insensitive match, or the full record word sequence for a
            dropped-prefix tail of at least three characters). Never true when the gate is closed or the answer is UNKNOWN.
        soft_module_sha256 (str | Unset): hybrid_research profiles only. SHA256 of the released reader weights that
            produced the soft rows (memory_module_sha256 is the research reader's).
        answer_path (MemorillaGenerateResponseAnswerPath | Unset): Present in source-verified mode and identifies the
            actual published answer path.
        verification_status (MemorillaGenerateResponseVerificationStatus | Unset): Model-supported means a completed
            verifier emitted the exact positive support label. It is a model judgment, not a formal entailment proof or a
            guarantee that caller-provided source facts are true.
        source_alignment (MemorillaGenerateResponseSourceAlignment | Unset): Source text and embedding row association
            was supplied by the caller; native embeddings are not recomputed to certify it.
        document_text_sha256s (list[str] | Unset): Source-verified mode only. SHA256 of each original UTF-8 source text,
            preserving row order; identifies supplied bytes, not source authority or selected supporting citations.
        diagnostics (MemorillaDiagnostics | Unset): hybrid_research profiles with include_diagnostics only: how the span
            hybrid read the request. Fields that do not apply are absent (never
            null). Margins are score - null_score - the bundle's span gate
            threshold; the cascade widens when the narrow margin is below
            cascade_threshold, and the gate opens when margin >= gate_offset.
            Research diagnostics, not a stable contract.
    """

    model: str
    text: str
    token_ids: list[int]
    finish_reason: MemorillaGenerateResponseFinishReason
    input_documents: int
    memory_tokens: MemorillaGenerateResponseMemoryTokens
    context_source: MemorillaGenerateResponseContextSource
    encoder_execution_identity: str
    decoder_execution_identity: str
    memory_module_sha256: str
    usage: MemorillaGenerateUsage
    candidate_sha256: str | Unset = UNSET
    answer_guard: MemorillaGenerateResponseAnswerGuard | Unset = UNSET
    citations: list[MemorillaCitation] | Unset = UNSET
    citations_unavailable: bool | Unset = UNSET
    record_gate: MemorillaGenerateResponseRecordGate | Unset = UNSET
    canonicalized: bool | Unset = UNSET
    soft_module_sha256: str | Unset = UNSET
    answer_path: MemorillaGenerateResponseAnswerPath | Unset = UNSET
    verification_status: MemorillaGenerateResponseVerificationStatus | Unset = UNSET
    source_alignment: MemorillaGenerateResponseSourceAlignment | Unset = UNSET
    document_text_sha256s: list[str] | Unset = UNSET
    diagnostics: MemorillaDiagnostics | Unset = UNSET

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

        candidate_sha256 = self.candidate_sha256

        answer_guard: str | Unset = UNSET
        if not isinstance(self.answer_guard, Unset):
            answer_guard = self.answer_guard.value

        citations: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.citations, Unset):
            citations = []
            for citations_item_data in self.citations:
                citations_item = citations_item_data.to_dict()
                citations.append(citations_item)

        citations_unavailable = self.citations_unavailable

        record_gate: str | Unset = UNSET
        if not isinstance(self.record_gate, Unset):
            record_gate = self.record_gate.value

        canonicalized = self.canonicalized

        soft_module_sha256 = self.soft_module_sha256

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

        diagnostics: dict[str, Any] | Unset = UNSET
        if not isinstance(self.diagnostics, Unset):
            diagnostics = self.diagnostics.to_dict()

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
        if candidate_sha256 is not UNSET:
            field_dict["candidate_sha256"] = candidate_sha256
        if answer_guard is not UNSET:
            field_dict["answer_guard"] = answer_guard
        if citations is not UNSET:
            field_dict["citations"] = citations
        if citations_unavailable is not UNSET:
            field_dict["citations_unavailable"] = citations_unavailable
        if record_gate is not UNSET:
            field_dict["record_gate"] = record_gate
        if canonicalized is not UNSET:
            field_dict["canonicalized"] = canonicalized
        if soft_module_sha256 is not UNSET:
            field_dict["soft_module_sha256"] = soft_module_sha256
        if answer_path is not UNSET:
            field_dict["answer_path"] = answer_path
        if verification_status is not UNSET:
            field_dict["verification_status"] = verification_status
        if source_alignment is not UNSET:
            field_dict["source_alignment"] = source_alignment
        if document_text_sha256s is not UNSET:
            field_dict["document_text_sha256s"] = document_text_sha256s
        if diagnostics is not UNSET:
            field_dict["diagnostics"] = diagnostics

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memorilla_citation import MemorillaCitation
        from ..models.memorilla_diagnostics import MemorillaDiagnostics
        from ..models.memorilla_generate_usage import MemorillaGenerateUsage

        d = dict(src_dict)
        model = d.pop("model")

        text = d.pop("text")

        token_ids = cast(list[int], d.pop("token_ids"))

        finish_reason = MemorillaGenerateResponseFinishReason(d.pop("finish_reason"))

        input_documents = d.pop("input_documents")

        memory_tokens = MemorillaGenerateResponseMemoryTokens(d.pop("memory_tokens"))

        context_source = MemorillaGenerateResponseContextSource(d.pop("context_source"))

        encoder_execution_identity = d.pop("encoder_execution_identity")

        decoder_execution_identity = d.pop("decoder_execution_identity")

        memory_module_sha256 = d.pop("memory_module_sha256")

        usage = MemorillaGenerateUsage.from_dict(d.pop("usage"))

        candidate_sha256 = d.pop("candidate_sha256", UNSET)

        _answer_guard = d.pop("answer_guard", UNSET)
        answer_guard: MemorillaGenerateResponseAnswerGuard | Unset
        if isinstance(_answer_guard, Unset):
            answer_guard = UNSET
        else:
            answer_guard = MemorillaGenerateResponseAnswerGuard(_answer_guard)

        _citations = d.pop("citations", UNSET)
        citations: list[MemorillaCitation] | Unset = UNSET
        if _citations is not UNSET:
            citations = []
            for citations_item_data in _citations:
                citations_item = MemorillaCitation.from_dict(citations_item_data)

                citations.append(citations_item)

        citations_unavailable = d.pop("citations_unavailable", UNSET)

        _record_gate = d.pop("record_gate", UNSET)
        record_gate: MemorillaGenerateResponseRecordGate | Unset
        if isinstance(_record_gate, Unset):
            record_gate = UNSET
        else:
            record_gate = MemorillaGenerateResponseRecordGate(_record_gate)

        canonicalized = d.pop("canonicalized", UNSET)

        soft_module_sha256 = d.pop("soft_module_sha256", UNSET)

        _answer_path = d.pop("answer_path", UNSET)
        answer_path: MemorillaGenerateResponseAnswerPath | Unset
        if isinstance(_answer_path, Unset):
            answer_path = UNSET
        else:
            answer_path = MemorillaGenerateResponseAnswerPath(_answer_path)

        _verification_status = d.pop("verification_status", UNSET)
        verification_status: MemorillaGenerateResponseVerificationStatus | Unset
        if isinstance(_verification_status, Unset):
            verification_status = UNSET
        else:
            verification_status = MemorillaGenerateResponseVerificationStatus(_verification_status)

        _source_alignment = d.pop("source_alignment", UNSET)
        source_alignment: MemorillaGenerateResponseSourceAlignment | Unset
        if isinstance(_source_alignment, Unset):
            source_alignment = UNSET
        else:
            source_alignment = MemorillaGenerateResponseSourceAlignment(_source_alignment)

        document_text_sha256s = cast(list[str], d.pop("document_text_sha256s", UNSET))

        _diagnostics = d.pop("diagnostics", UNSET)
        diagnostics: MemorillaDiagnostics | Unset
        if isinstance(_diagnostics, Unset):
            diagnostics = UNSET
        else:
            diagnostics = MemorillaDiagnostics.from_dict(_diagnostics)

        memorilla_generate_response = cls(
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
            candidate_sha256=candidate_sha256,
            answer_guard=answer_guard,
            citations=citations,
            citations_unavailable=citations_unavailable,
            record_gate=record_gate,
            canonicalized=canonicalized,
            soft_module_sha256=soft_module_sha256,
            answer_path=answer_path,
            verification_status=verification_status,
            source_alignment=source_alignment,
            document_text_sha256s=document_text_sha256s,
            diagnostics=diagnostics,
        )

        return memorilla_generate_response
