from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_retained_dataset_response_200_initial_history import QueryRetainedDatasetResponse200InitialHistory

if TYPE_CHECKING:
    from ..models.query_retained_dataset_response_200_measures_item import QueryRetainedDatasetResponse200MeasuresItem
    from ..models.query_retained_dataset_response_200_rows_item import QueryRetainedDatasetResponse200RowsItem


T = TypeVar("T", bound="QueryRetainedDatasetResponse200")


@_attrs_define
class QueryRetainedDatasetResponse200:
    """
    Attributes:
        next_ (None | str): Exclusive continuation key; null when complete. An empty page may still have a next key.
        initial_history (QueryRetainedDatasetResponse200InitialHistory):
        dimensions (list[str]):
        measures (list[QueryRetainedDatasetResponse200MeasuresItem]):
        rows (list[QueryRetainedDatasetResponse200RowsItem]):
    """

    next_: None | str
    initial_history: QueryRetainedDatasetResponse200InitialHistory
    dimensions: list[str]
    measures: list[QueryRetainedDatasetResponse200MeasuresItem]
    rows: list[QueryRetainedDatasetResponse200RowsItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        next_: None | str
        next_ = self.next_

        initial_history = self.initial_history.value

        dimensions = self.dimensions

        measures = []
        for measures_item_data in self.measures:
            measures_item = measures_item_data.to_dict()
            measures.append(measures_item)

        rows = []
        for rows_item_data in self.rows:
            rows_item = rows_item_data.to_dict()
            rows.append(rows_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "next": next_,
                "initial_history": initial_history,
                "dimensions": dimensions,
                "measures": measures,
                "rows": rows,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_retained_dataset_response_200_measures_item import (
            QueryRetainedDatasetResponse200MeasuresItem,
        )
        from ..models.query_retained_dataset_response_200_rows_item import QueryRetainedDatasetResponse200RowsItem

        d = dict(src_dict)

        def _parse_next_(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        next_ = _parse_next_(d.pop("next"))

        initial_history = QueryRetainedDatasetResponse200InitialHistory(d.pop("initial_history"))

        dimensions = cast(list[str], d.pop("dimensions"))

        measures = []
        _measures = d.pop("measures")
        for measures_item_data in _measures:
            measures_item = QueryRetainedDatasetResponse200MeasuresItem.from_dict(measures_item_data)

            measures.append(measures_item)

        rows = []
        _rows = d.pop("rows")
        for rows_item_data in _rows:
            rows_item = QueryRetainedDatasetResponse200RowsItem.from_dict(rows_item_data)

            rows.append(rows_item)

        query_retained_dataset_response_200 = cls(
            next_=next_,
            initial_history=initial_history,
            dimensions=dimensions,
            measures=measures,
            rows=rows,
        )

        query_retained_dataset_response_200.additional_properties = d
        return query_retained_dataset_response_200

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
