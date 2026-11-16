from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ChunkDiarizationConfig")


@_attrs_define
class ChunkDiarizationConfig:
    """Source-wide online speaker clustering on a fixed 1.5 s grid with 3 s embedding windows. Identities are stable within
    a source and recipe. Silence and ambiguous voices remain unassigned. Analysis advances incrementally; admitted
    cached timelines and durable centroid/sample-position checkpoints are shared across matching outputs and
    continuations. Missing checkpoints reconstruct the required prefix deterministically.

        Attributes:
            model (str | Unset): Speaker ONNX registry reference; defaults to 3D-Speaker CAM++. For kind=diarize the target
                model selects the speaker model.
            model_identity (str | Unset): Optional SHA-256 pin of the speaker ONNX file, including when paired with a
                transcription model.
            similarity_threshold (float | Unset):  Default: 0.45.
            ambiguity_margin (float | Unset):  Default: 0.03.
            min_rms (float | Unset):  Default: 0.01.
            max_speakers (int | Unset):  Default: 64.
    """

    model: str | Unset = UNSET
    model_identity: str | Unset = UNSET
    similarity_threshold: float | Unset = 0.45
    ambiguity_margin: float | Unset = 0.03
    min_rms: float | Unset = 0.01
    max_speakers: int | Unset = 64
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        model = self.model

        model_identity = self.model_identity

        similarity_threshold = self.similarity_threshold

        ambiguity_margin = self.ambiguity_margin

        min_rms = self.min_rms

        max_speakers = self.max_speakers

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if model is not UNSET:
            field_dict["model"] = model
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if similarity_threshold is not UNSET:
            field_dict["similarity_threshold"] = similarity_threshold
        if ambiguity_margin is not UNSET:
            field_dict["ambiguity_margin"] = ambiguity_margin
        if min_rms is not UNSET:
            field_dict["min_rms"] = min_rms
        if max_speakers is not UNSET:
            field_dict["max_speakers"] = max_speakers

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        model = d.pop("model", UNSET)

        model_identity = d.pop("model_identity", UNSET)

        similarity_threshold = d.pop("similarity_threshold", UNSET)

        ambiguity_margin = d.pop("ambiguity_margin", UNSET)

        min_rms = d.pop("min_rms", UNSET)

        max_speakers = d.pop("max_speakers", UNSET)

        chunk_diarization_config = cls(
            model=model,
            model_identity=model_identity,
            similarity_threshold=similarity_threshold,
            ambiguity_margin=ambiguity_margin,
            min_rms=min_rms,
            max_speakers=max_speakers,
        )

        chunk_diarization_config.additional_properties = d
        return chunk_diarization_config

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
