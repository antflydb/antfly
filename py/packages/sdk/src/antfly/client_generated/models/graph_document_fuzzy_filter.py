from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.fuzziness_type_1 import FuzzinessType1
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphDocumentFuzzyFilter")


@_attrs_define
class GraphDocumentFuzzyFilter:
    """
    Attributes:
        term (str):
        field (str):
        fuzziness (FuzzinessType1 | int): The fuzziness of the query. Can be an integer or "auto".
        prefix_length (int | Unset):
    """

    term: str
    field: str
    fuzziness: FuzzinessType1 | int
    prefix_length: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        term = self.term

        field = self.field

        fuzziness: int | str
        if isinstance(self.fuzziness, FuzzinessType1):
            fuzziness = self.fuzziness.value
        else:
            fuzziness = self.fuzziness

        prefix_length = self.prefix_length

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "term": term,
                "field": field,
                "fuzziness": fuzziness,
            }
        )
        if prefix_length is not UNSET:
            field_dict["prefix_length"] = prefix_length

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        term = d.pop("term")

        field = d.pop("field")

        def _parse_fuzziness(data: object) -> FuzzinessType1 | int:
            try:
                if not isinstance(data, str):
                    raise TypeError()
                componentsschemas_fuzziness_type_1 = FuzzinessType1(data)

                return componentsschemas_fuzziness_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(FuzzinessType1 | int, data)

        fuzziness = _parse_fuzziness(d.pop("fuzziness"))

        prefix_length = d.pop("prefix_length", UNSET)

        graph_document_fuzzy_filter = cls(
            term=term,
            field=field,
            fuzziness=fuzziness,
            prefix_length=prefix_length,
        )

        return graph_document_fuzzy_filter
