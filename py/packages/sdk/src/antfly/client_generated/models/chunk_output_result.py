from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_output_result_kind import ChunkOutputResultKind
from ..models.chunk_output_result_speaker_scope import ChunkOutputResultSpeakerScope
from ..models.chunk_output_result_status import ChunkOutputResultStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_output_label import ChunkOutputLabel
    from ..models.chunk_speaker_span import ChunkSpeakerSpan
    from ..models.chunk_transcript_span import ChunkTranscriptSpan


T = TypeVar("T", bound="ChunkOutputResult")


@_attrs_define
class ChunkOutputResult:
    """
    Attributes:
        name (str):
        kind (ChunkOutputResultKind):
        required (bool):
        model (str):
        model_identity (str):
        model_revision (str):
        recipe_id (str): Independent output identity over resolved model and execution options; requiredness is a
            coverage policy.
        sub_index (int):
        status (ChunkOutputResultStatus):
        speaker_model_revision (str | Unset): SHA-256 of the speaker ONNX bytes used for this output. Included in recipe
            identity for diarization and paired transcription or embedding boundaries.
        speaker_scope (ChunkOutputResultSpeakerScope | Unset): Speaker indices are shared across all windows of this
            source and diarization recipe.
        speaker_spans (list[ChunkSpeakerSpan] | Unset): Timed voice annotations for kind=diarize, including unassigned
            intervals. No transcript is required.
        start_ns (int | Unset):
        end_ns (int | Unset):
        attempts (int | Unset): Durable repair attempts made by the enrichment consumer.
        terminal (bool | Unset): True when the configured repair budget is exhausted.
        code (str | Unset):
        embedding (list[float] | Unset):
        spans (list[ChunkTranscriptSpan] | Unset):
        labels (list[ChunkOutputLabel] | Unset):
    """

    name: str
    kind: ChunkOutputResultKind
    required: bool
    model: str
    model_identity: str
    model_revision: str
    recipe_id: str
    sub_index: int
    status: ChunkOutputResultStatus
    speaker_model_revision: str | Unset = UNSET
    speaker_scope: ChunkOutputResultSpeakerScope | Unset = UNSET
    speaker_spans: list[ChunkSpeakerSpan] | Unset = UNSET
    start_ns: int | Unset = UNSET
    end_ns: int | Unset = UNSET
    attempts: int | Unset = UNSET
    terminal: bool | Unset = UNSET
    code: str | Unset = UNSET
    embedding: list[float] | Unset = UNSET
    spans: list[ChunkTranscriptSpan] | Unset = UNSET
    labels: list[ChunkOutputLabel] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        required = self.required

        model = self.model

        model_identity = self.model_identity

        model_revision = self.model_revision

        recipe_id = self.recipe_id

        sub_index = self.sub_index

        status = self.status.value

        speaker_model_revision = self.speaker_model_revision

        speaker_scope: str | Unset = UNSET
        if not isinstance(self.speaker_scope, Unset):
            speaker_scope = self.speaker_scope.value

        speaker_spans: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.speaker_spans, Unset):
            speaker_spans = []
            for speaker_spans_item_data in self.speaker_spans:
                speaker_spans_item = speaker_spans_item_data.to_dict()
                speaker_spans.append(speaker_spans_item)

        start_ns = self.start_ns

        end_ns = self.end_ns

        attempts = self.attempts

        terminal = self.terminal

        code = self.code

        embedding: list[float] | Unset = UNSET
        if not isinstance(self.embedding, Unset):
            embedding = self.embedding

        spans: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.spans, Unset):
            spans = []
            for spans_item_data in self.spans:
                spans_item = spans_item_data.to_dict()
                spans.append(spans_item)

        labels: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.labels, Unset):
            labels = []
            for labels_item_data in self.labels:
                labels_item = labels_item_data.to_dict()
                labels.append(labels_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "required": required,
                "model": model,
                "model_identity": model_identity,
                "model_revision": model_revision,
                "recipe_id": recipe_id,
                "sub_index": sub_index,
                "status": status,
            }
        )
        if speaker_model_revision is not UNSET:
            field_dict["speaker_model_revision"] = speaker_model_revision
        if speaker_scope is not UNSET:
            field_dict["speaker_scope"] = speaker_scope
        if speaker_spans is not UNSET:
            field_dict["speaker_spans"] = speaker_spans
        if start_ns is not UNSET:
            field_dict["start_ns"] = start_ns
        if end_ns is not UNSET:
            field_dict["end_ns"] = end_ns
        if attempts is not UNSET:
            field_dict["attempts"] = attempts
        if terminal is not UNSET:
            field_dict["terminal"] = terminal
        if code is not UNSET:
            field_dict["code"] = code
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if spans is not UNSET:
            field_dict["spans"] = spans
        if labels is not UNSET:
            field_dict["labels"] = labels

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_output_label import ChunkOutputLabel
        from ..models.chunk_speaker_span import ChunkSpeakerSpan
        from ..models.chunk_transcript_span import ChunkTranscriptSpan

        d = dict(src_dict)
        name = d.pop("name")

        kind = ChunkOutputResultKind(d.pop("kind"))

        required = d.pop("required")

        model = d.pop("model")

        model_identity = d.pop("model_identity")

        model_revision = d.pop("model_revision")

        recipe_id = d.pop("recipe_id")

        sub_index = d.pop("sub_index")

        status = ChunkOutputResultStatus(d.pop("status"))

        speaker_model_revision = d.pop("speaker_model_revision", UNSET)

        _speaker_scope = d.pop("speaker_scope", UNSET)
        speaker_scope: ChunkOutputResultSpeakerScope | Unset
        if isinstance(_speaker_scope, Unset):
            speaker_scope = UNSET
        else:
            speaker_scope = ChunkOutputResultSpeakerScope(_speaker_scope)

        _speaker_spans = d.pop("speaker_spans", UNSET)
        speaker_spans: list[ChunkSpeakerSpan] | Unset = UNSET
        if _speaker_spans is not UNSET:
            speaker_spans = []
            for speaker_spans_item_data in _speaker_spans:
                speaker_spans_item = ChunkSpeakerSpan.from_dict(speaker_spans_item_data)

                speaker_spans.append(speaker_spans_item)

        start_ns = d.pop("start_ns", UNSET)

        end_ns = d.pop("end_ns", UNSET)

        attempts = d.pop("attempts", UNSET)

        terminal = d.pop("terminal", UNSET)

        code = d.pop("code", UNSET)

        embedding = cast(list[float], d.pop("embedding", UNSET))

        _spans = d.pop("spans", UNSET)
        spans: list[ChunkTranscriptSpan] | Unset = UNSET
        if _spans is not UNSET:
            spans = []
            for spans_item_data in _spans:
                spans_item = ChunkTranscriptSpan.from_dict(spans_item_data)

                spans.append(spans_item)

        _labels = d.pop("labels", UNSET)
        labels: list[ChunkOutputLabel] | Unset = UNSET
        if _labels is not UNSET:
            labels = []
            for labels_item_data in _labels:
                labels_item = ChunkOutputLabel.from_dict(labels_item_data)

                labels.append(labels_item)

        chunk_output_result = cls(
            name=name,
            kind=kind,
            required=required,
            model=model,
            model_identity=model_identity,
            model_revision=model_revision,
            recipe_id=recipe_id,
            sub_index=sub_index,
            status=status,
            speaker_model_revision=speaker_model_revision,
            speaker_scope=speaker_scope,
            speaker_spans=speaker_spans,
            start_ns=start_ns,
            end_ns=end_ns,
            attempts=attempts,
            terminal=terminal,
            code=code,
            embedding=embedding,
            spans=spans,
            labels=labels,
        )

        chunk_output_result.additional_properties = d
        return chunk_output_result

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
