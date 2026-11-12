from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retained_dataset_info_initial_history import RetainedDatasetInfoInitialHistory
from ..models.retained_dataset_info_phase import RetainedDatasetInfoPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.retained_dataset_measure import RetainedDatasetMeasure
    from ..models.retained_dataset_progress import RetainedDatasetProgress


T = TypeVar("T", bound="RetainedDatasetInfo")


@_attrs_define
class RetainedDatasetInfo:
    """
    Attributes:
        id (str): Stable dataset identifier in hexadecimal.
        name (str):
        revision (int):
        phase (RetainedDatasetInfoPhase):
        source_released (bool): True after verified native revocation or archive handoff releases the source dependency.
            Retiring remains a durable discard tombstone while archive collection proceeds independently.
        initial_history (RetainedDatasetInfoInitialHistory):
        dimensions (list[str]):
        measures (list[RetainedDatasetMeasure]):
        retention_ns (str):
        interval_ns (str):
        origin_ns (str):
        progress (None | RetainedDatasetProgress | Unset):
    """

    id: str
    name: str
    revision: int
    phase: RetainedDatasetInfoPhase
    source_released: bool
    initial_history: RetainedDatasetInfoInitialHistory
    dimensions: list[str]
    measures: list[RetainedDatasetMeasure]
    retention_ns: str
    interval_ns: str
    origin_ns: str
    progress: None | RetainedDatasetProgress | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.retained_dataset_progress import RetainedDatasetProgress

        id = self.id

        name = self.name

        revision = self.revision

        phase = self.phase.value

        source_released = self.source_released

        initial_history = self.initial_history.value

        dimensions = self.dimensions

        measures = []
        for measures_item_data in self.measures:
            measures_item = measures_item_data.to_dict()
            measures.append(measures_item)

        retention_ns = self.retention_ns

        interval_ns = self.interval_ns

        origin_ns = self.origin_ns

        progress: dict[str, Any] | None | Unset
        if isinstance(self.progress, Unset):
            progress = UNSET
        elif isinstance(self.progress, RetainedDatasetProgress):
            progress = self.progress.to_dict()
        else:
            progress = self.progress

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "revision": revision,
                "phase": phase,
                "source_released": source_released,
                "initial_history": initial_history,
                "dimensions": dimensions,
                "measures": measures,
                "retention_ns": retention_ns,
                "interval_ns": interval_ns,
                "origin_ns": origin_ns,
            }
        )
        if progress is not UNSET:
            field_dict["progress"] = progress

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retained_dataset_measure import RetainedDatasetMeasure
        from ..models.retained_dataset_progress import RetainedDatasetProgress

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        revision = d.pop("revision")

        phase = RetainedDatasetInfoPhase(d.pop("phase"))

        source_released = d.pop("source_released")

        initial_history = RetainedDatasetInfoInitialHistory(d.pop("initial_history"))

        dimensions = cast(list[str], d.pop("dimensions"))

        measures = []
        _measures = d.pop("measures")
        for measures_item_data in _measures:
            measures_item = RetainedDatasetMeasure.from_dict(measures_item_data)

            measures.append(measures_item)

        retention_ns = d.pop("retention_ns")

        interval_ns = d.pop("interval_ns")

        origin_ns = d.pop("origin_ns")

        def _parse_progress(data: object) -> None | RetainedDatasetProgress | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                progress_type_1 = RetainedDatasetProgress.from_dict(data)

                return progress_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | RetainedDatasetProgress | Unset, data)

        progress = _parse_progress(d.pop("progress", UNSET))

        retained_dataset_info = cls(
            id=id,
            name=name,
            revision=revision,
            phase=phase,
            source_released=source_released,
            initial_history=initial_history,
            dimensions=dimensions,
            measures=measures,
            retention_ns=retention_ns,
            interval_ns=interval_ns,
            origin_ns=origin_ns,
            progress=progress,
        )

        retained_dataset_info.additional_properties = d
        return retained_dataset_info

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
