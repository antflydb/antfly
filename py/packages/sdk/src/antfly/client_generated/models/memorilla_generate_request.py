from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memorilla_generate_request_context_mode import MemorillaGenerateRequestContextMode
from ..models.memorilla_generate_request_read_mode import MemorillaGenerateRequestReadMode
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
    Profiles configured with reader_kind=encoder_lora_research or
    hybrid_research instead take document_texts in latent mode and encode
    them server-side (the hybrid computes the pooled document and question
    vectors itself as well); they reject document_embeddings and
    source_verified.

        Attributes:
            model (str): Name from inference.memorilla.profiles. Requests cannot override the bound model assets or memory
                module.
            question (str): A nonempty question admitted to the configured encoder's 256-token limit after canonical
                whitespace normalization.
            encoder_execution_identity (str): Exact model_identity returned by the configured encoder. For released profiles
                this is the qualified encoder identity; for encoder_lora_research profiles it is the fixed research encoder
                identity; for hybrid_research profiles it is the qualified (stock) encoder identity, because both passes run on
                the stock encoder. Ordinary mixed-precision encoder identities are incompatible.
            document_embeddings (list[list[float]] | Unset): Required for released profiles (reader_kind=released), which
                read stored features; omitted and rejected for encoder_lora_research profiles, which encode document_texts
                themselves. Values must be numeric tokens.
            max_new_tokens (int | Unset): Maximum tokens for each answer candidate, including an optional source repair.
                Source-verified mode generates a memory candidate and an independent source answer, with at most one source
                repair and two verifier calls of eight tokens each; usage reports all phases. Every phase also respects profile
                and operator ceilings. Default: 64.
            context_mode (MemorillaGenerateRequestContextMode | Unset): Latent uses stored features alone. Source-verified
                mode requires document_texts and explicitly checks or replaces the memory candidate using their contents.
                Default: MemorillaGenerateRequestContextMode.LATENT.
            document_texts (list[str] | Unset): Released profiles: required only for source_verified and rejected in
                latent mode. One nonblank UTF-8 source text per embedding row, in the
                same order; at most 8192 bytes each and 32768 source bytes total. The
                complete rendered prompt must fit the 1024-token internal bound and
                operator limits, without truncation. Pairing is asserted by the
                caller, not recomputed or authenticated.
                encoder_lora_research profiles: required in latent mode (1 to 64
                texts, same per-text and total byte limits); the server encodes
                each text with the merged research encoder.
                hybrid_research profiles: the same rules; the server pools each
                text with the stock encoder (texts over its 256-token pooled limit
                are split into sentence-greedy chunks of at most 128 tokens, one
                soft-memory vector each, at most 64 vectors) and encodes the
                per-token record batch with the run-time encoder LoRA (padded
                batch rows of at most 1024 tokens). With a span read cascade the
                texts are the ranked candidate list, best first: the soft half
                pools every text and the span reads take leading texts.
            read_mode (MemorillaGenerateRequestReadMode | Unset): hybrid_research profiles only (400 for other kinds).
                Absent keeps
                the bundle's default read (auto on a bundle with a span read
                cascade; every text on a span bundle without one). auto, narrow
                and wide need a bundle with a span read cascade (400 otherwise):
                narrow reads the top two texts with a 32-token window; wide reads
                texts in order while their encoder tokens stay within the
                cascade's budget, with its wider window; auto reads narrow and
                reads wide when the narrow margin is below the cascade threshold,
                reusing the narrow pass's work. none needs a span bundle and skips
                the span pass: the decoder reads the 16 soft rows alone under the
                released helpful prompt, the closed-gate path, i.e. stock
                Memorilla over the same pooled vectors (record_gate closed,
                memory_tokens 16, no citations).
            include_diagnostics (bool | Unset): hybrid_research profiles only (true is 400 for other kinds). Adds the
                diagnostics object to the response; the answer and every other field are unchanged. Default: False.
    """

    model: str
    question: str
    encoder_execution_identity: str
    document_embeddings: list[list[float]] | Unset = UNSET
    max_new_tokens: int | Unset = 64
    context_mode: MemorillaGenerateRequestContextMode | Unset = MemorillaGenerateRequestContextMode.LATENT
    document_texts: list[str] | Unset = UNSET
    read_mode: MemorillaGenerateRequestReadMode | Unset = UNSET
    include_diagnostics: bool | Unset = False

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        question = self.question

        encoder_execution_identity = self.encoder_execution_identity

        document_embeddings: list[list[float]] | Unset = UNSET
        if not isinstance(self.document_embeddings, Unset):
            document_embeddings = []
            for document_embeddings_item_data in self.document_embeddings:
                document_embeddings_item = document_embeddings_item_data

                document_embeddings.append(document_embeddings_item)

        max_new_tokens = self.max_new_tokens

        context_mode: str | Unset = UNSET
        if not isinstance(self.context_mode, Unset):
            context_mode = self.context_mode.value

        document_texts: list[str] | Unset = UNSET
        if not isinstance(self.document_texts, Unset):
            document_texts = self.document_texts

        read_mode: str | Unset = UNSET
        if not isinstance(self.read_mode, Unset):
            read_mode = self.read_mode.value

        include_diagnostics = self.include_diagnostics

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "model": model,
                "question": question,
                "encoder_execution_identity": encoder_execution_identity,
            }
        )
        if document_embeddings is not UNSET:
            field_dict["document_embeddings"] = document_embeddings
        if max_new_tokens is not UNSET:
            field_dict["max_new_tokens"] = max_new_tokens
        if context_mode is not UNSET:
            field_dict["context_mode"] = context_mode
        if document_texts is not UNSET:
            field_dict["document_texts"] = document_texts
        if read_mode is not UNSET:
            field_dict["read_mode"] = read_mode
        if include_diagnostics is not UNSET:
            field_dict["include_diagnostics"] = include_diagnostics

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model")

        question = d.pop("question")

        encoder_execution_identity = d.pop("encoder_execution_identity")

        _document_embeddings = d.pop("document_embeddings", UNSET)
        document_embeddings: list[list[float]] | Unset = UNSET
        if _document_embeddings is not UNSET:
            document_embeddings = []
            for document_embeddings_item_data in _document_embeddings:
                document_embeddings_item = cast(list[float], document_embeddings_item_data)

                document_embeddings.append(document_embeddings_item)

        max_new_tokens = d.pop("max_new_tokens", UNSET)

        _context_mode = d.pop("context_mode", UNSET)
        context_mode: MemorillaGenerateRequestContextMode | Unset
        if isinstance(_context_mode, Unset):
            context_mode = UNSET
        else:
            context_mode = MemorillaGenerateRequestContextMode(_context_mode)

        document_texts = cast(list[str], d.pop("document_texts", UNSET))

        _read_mode = d.pop("read_mode", UNSET)
        read_mode: MemorillaGenerateRequestReadMode | Unset
        if isinstance(_read_mode, Unset):
            read_mode = UNSET
        else:
            read_mode = MemorillaGenerateRequestReadMode(_read_mode)

        include_diagnostics = d.pop("include_diagnostics", UNSET)

        memorilla_generate_request = cls(
            model=model,
            question=question,
            encoder_execution_identity=encoder_execution_identity,
            document_embeddings=document_embeddings,
            max_new_tokens=max_new_tokens,
            context_mode=context_mode,
            document_texts=document_texts,
            read_mode=read_mode,
            include_diagnostics=include_diagnostics,
        )

        return memorilla_generate_request
