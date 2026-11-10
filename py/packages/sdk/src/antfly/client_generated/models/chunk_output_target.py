from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chunk_output_target_kind import ChunkOutputTargetKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chunk_diarization_config import ChunkDiarizationConfig
    from ..models.chunk_output_target_skip_windows_item import ChunkOutputTargetSkipWindowsItem


T = TypeVar("T", bound="ChunkOutputTarget")


@_attrs_define
class ChunkOutputTarget:
    """
    Attributes:
        name (str):
        kind (ChunkOutputTargetKind):
        model (str):
        execute (bool | Unset): False retains this model as a planning boundary while skipping its inference; used for
            independent output repairs. Default: True.
        skip_windows (list[ChunkOutputTargetSkipWindowsItem] | Unset): Execution-only repair mask for already accepted
            sub-windows; does not affect the model recipe or boundary plan. Skipped windows return receipts with status
            skipped.
        required (bool | Unset):  Default: True.
        model_identity (str | Unset):
        task_type (str | Unset):  Default: 'RETRIEVAL_DOCUMENT'.
        dimensions (int | Unset):
        language (str | Unset):
        diarize (bool | Unset): Convenience option on transcription outputs; enables source-wide diarization with the
            default speaker model. Default: False.
        diarization (ChunkDiarizationConfig | Unset): Source-wide online speaker clustering on a fixed 1.5 s grid with 3
            s embedding windows. Identities are stable within a source and recipe. Silence and ambiguous voices remain
            unassigned. Analysis advances incrementally; admitted cached timelines and durable centroid/sample-position
            checkpoints are shared across matching outputs and continuations. Missing checkpoints reconstruct the required
            prefix deterministically.
        top_k (int | Unset):  Default: 5.
    """

    name: str
    kind: ChunkOutputTargetKind
    model: str
    execute: bool | Unset = True
    skip_windows: list[ChunkOutputTargetSkipWindowsItem] | Unset = UNSET
    required: bool | Unset = True
    model_identity: str | Unset = UNSET
    task_type: str | Unset = "RETRIEVAL_DOCUMENT"
    dimensions: int | Unset = UNSET
    language: str | Unset = UNSET
    diarize: bool | Unset = False
    diarization: ChunkDiarizationConfig | Unset = UNSET
    top_k: int | Unset = 5
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        model = self.model

        execute = self.execute

        skip_windows: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.skip_windows, Unset):
            skip_windows = []
            for skip_windows_item_data in self.skip_windows:
                skip_windows_item = skip_windows_item_data.to_dict()
                skip_windows.append(skip_windows_item)

        required = self.required

        model_identity = self.model_identity

        task_type = self.task_type

        dimensions = self.dimensions

        language = self.language

        diarize = self.diarize

        diarization: dict[str, Any] | Unset = UNSET
        if not isinstance(self.diarization, Unset):
            diarization = self.diarization.to_dict()

        top_k = self.top_k

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
                "model": model,
            }
        )
        if execute is not UNSET:
            field_dict["execute"] = execute
        if skip_windows is not UNSET:
            field_dict["skip_windows"] = skip_windows
        if required is not UNSET:
            field_dict["required"] = required
        if model_identity is not UNSET:
            field_dict["model_identity"] = model_identity
        if task_type is not UNSET:
            field_dict["task_type"] = task_type
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if language is not UNSET:
            field_dict["language"] = language
        if diarize is not UNSET:
            field_dict["diarize"] = diarize
        if diarization is not UNSET:
            field_dict["diarization"] = diarization
        if top_k is not UNSET:
            field_dict["top_k"] = top_k

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_diarization_config import ChunkDiarizationConfig
        from ..models.chunk_output_target_skip_windows_item import ChunkOutputTargetSkipWindowsItem

        d = dict(src_dict)
        name = d.pop("name")

        kind = ChunkOutputTargetKind(d.pop("kind"))

        model = d.pop("model")

        execute = d.pop("execute", UNSET)

        _skip_windows = d.pop("skip_windows", UNSET)
        skip_windows: list[ChunkOutputTargetSkipWindowsItem] | Unset = UNSET
        if _skip_windows is not UNSET:
            skip_windows = []
            for skip_windows_item_data in _skip_windows:
                skip_windows_item = ChunkOutputTargetSkipWindowsItem.from_dict(skip_windows_item_data)

                skip_windows.append(skip_windows_item)

        required = d.pop("required", UNSET)

        model_identity = d.pop("model_identity", UNSET)

        task_type = d.pop("task_type", UNSET)

        dimensions = d.pop("dimensions", UNSET)

        language = d.pop("language", UNSET)

        diarize = d.pop("diarize", UNSET)

        _diarization = d.pop("diarization", UNSET)
        diarization: ChunkDiarizationConfig | Unset
        if isinstance(_diarization, Unset):
            diarization = UNSET
        else:
            diarization = ChunkDiarizationConfig.from_dict(_diarization)

        top_k = d.pop("top_k", UNSET)

        chunk_output_target = cls(
            name=name,
            kind=kind,
            model=model,
            execute=execute,
            skip_windows=skip_windows,
            required=required,
            model_identity=model_identity,
            task_type=task_type,
            dimensions=dimensions,
            language=language,
            diarize=diarize,
            diarization=diarization,
            top_k=top_k,
        )

        chunk_output_target.additional_properties = d
        return chunk_output_target

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
