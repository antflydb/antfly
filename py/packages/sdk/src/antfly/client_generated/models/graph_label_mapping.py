from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.catalog_table_target import CatalogTableTarget
    from ..models.graph_label_mapping_where import GraphLabelMappingWhere


T = TypeVar("T", bound="GraphLabelMapping")


@_attrs_define
class GraphLabelMapping:
    """
    Attributes:
        name (str):
        table (CatalogTableTarget | str): A literal default.public table name or an explicitly scoped table target.
            Bindings retain the physical table incarnation through renames.
        where (GraphLabelMappingWhere | Unset): Equality predicates defining this label over stored document properties.
    """

    name: str
    table: CatalogTableTarget | str
    where: GraphLabelMappingWhere | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.catalog_table_target import CatalogTableTarget

        name = self.name

        table: dict[str, Any] | str
        if isinstance(self.table, CatalogTableTarget):
            table = self.table.to_dict()
        else:
            table = self.table

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "table": table,
            }
        )
        if where is not UNSET:
            field_dict["where"] = where

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.catalog_table_target import CatalogTableTarget
        from ..models.graph_label_mapping_where import GraphLabelMappingWhere

        d = dict(src_dict)
        name = d.pop("name")

        def _parse_table(data: object) -> CatalogTableTarget | str:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                table_type_1 = CatalogTableTarget.from_dict(data)

                return table_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CatalogTableTarget | str, data)

        table = _parse_table(d.pop("table"))

        _where = d.pop("where", UNSET)
        where: GraphLabelMappingWhere | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = GraphLabelMappingWhere.from_dict(_where)

        graph_label_mapping = cls(
            name=name,
            table=table,
            where=where,
        )

        return graph_label_mapping
