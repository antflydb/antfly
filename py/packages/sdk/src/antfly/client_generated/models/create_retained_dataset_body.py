from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_retained_dataset_body_source import CreateRetainedDatasetBodySource
    from ..models.retained_dataset_measure import RetainedDatasetMeasure


T = TypeVar("T", bound="CreateRetainedDatasetBody")


@_attrs_define
class CreateRetainedDatasetBody:
    """
    Attributes:
        source (CreateRetainedDatasetBodySource):
        timestamp (str):
        interval_ns (str): Positive bucket width in nanoseconds as a decimal string.
        mutable_for_ns (str):
        retention_ns (str):
        measures (list[RetainedDatasetMeasure]):
        dimensions (list[str] | Unset):
        origin_ns (str | Unset): Signed UTC nanoseconds as a decimal string. Default: '0'.
    """

    source: CreateRetainedDatasetBodySource
    timestamp: str
    interval_ns: str
    mutable_for_ns: str
    retention_ns: str
    measures: list[RetainedDatasetMeasure]
    dimensions: list[str] | Unset = UNSET
    origin_ns: str | Unset = "0"

    def to_dict(self) -> dict[str, Any]:
        source = self.source.to_dict()

        timestamp = self.timestamp

        interval_ns = self.interval_ns

        mutable_for_ns = self.mutable_for_ns

        retention_ns = self.retention_ns

        measures = []
        for measures_item_data in self.measures:
            measures_item = measures_item_data.to_dict()
            measures.append(measures_item)

        dimensions: list[str] | Unset = UNSET
        if not isinstance(self.dimensions, Unset):
            dimensions = self.dimensions

        origin_ns = self.origin_ns

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "source": source,
                "timestamp": timestamp,
                "interval_ns": interval_ns,
                "mutable_for_ns": mutable_for_ns,
                "retention_ns": retention_ns,
                "measures": measures,
            }
        )
        if dimensions is not UNSET:
            field_dict["dimensions"] = dimensions
        if origin_ns is not UNSET:
            field_dict["origin_ns"] = origin_ns

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_retained_dataset_body_source import CreateRetainedDatasetBodySource
        from ..models.retained_dataset_measure import RetainedDatasetMeasure

        d = dict(src_dict)
        source = CreateRetainedDatasetBodySource.from_dict(d.pop("source"))

        timestamp = d.pop("timestamp")

        interval_ns = d.pop("interval_ns")

        mutable_for_ns = d.pop("mutable_for_ns")

        retention_ns = d.pop("retention_ns")

        measures = []
        _measures = d.pop("measures")
        for measures_item_data in _measures:
            measures_item = RetainedDatasetMeasure.from_dict(measures_item_data)

            measures.append(measures_item)

        dimensions = cast(list[str], d.pop("dimensions", UNSET))

        origin_ns = d.pop("origin_ns", UNSET)

        create_retained_dataset_body = cls(
            source=source,
            timestamp=timestamp,
            interval_ns=interval_ns,
            mutable_for_ns=mutable_for_ns,
            retention_ns=retention_ns,
            measures=measures,
            dimensions=dimensions,
            origin_ns=origin_ns,
        )

        return create_retained_dataset_body
