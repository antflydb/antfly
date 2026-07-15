from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_query_request_total_mode import RowsQueryRequestTotalMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_array_length_projection import RowsArrayLengthProjection
    from ..models.rows_coalesce_projection import RowsCoalesceProjection
    from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_projection import RowsExpressionProjection
    from ..models.rows_expression_value import RowsExpressionValue
    from ..models.rows_field_alias_projection import RowsFieldAliasProjection
    from ..models.rows_json_extract_projection import RowsJsonExtractProjection
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_row_claim import RowsRowClaim
    from ..models.rows_where_type_0 import RowsWhereType0


T = TypeVar("T", bound="RowsQueryRequest")


@_attrs_define
class RowsQueryRequest:
    """Typed relational row-query plan. Predicate and expression arrays carry
    Antfly row-expression AST nodes; SQL syntax is adapter sugar over this
    native request shape.

        Attributes:
            source_cte (str | Unset): Optional ordered CTE name to read instead of the base table.
            where (Any | RowsWhereType0 | Unset): Canonical row predicate tree. A top-level `where` is one predicate
                atom, an `all` conjunction of atoms, `any` / `not` branch groups, or an
                `all` conjunction plus branch groups. Branches may contain scalar,
                membership, array, JSON, and text-pattern atoms; the server stores
                branches containing structured atoms in native mixed access predicate
                groups and keeps scalar-only branches in scalar predicate groups.
            expression_where (list[RowsExpressionCondition] | Unset): All computed expression predicates that must pass.
            expression_any (list[RowsExpressionConditionGroup] | Unset): OR groups of computed expression predicates.
            expression_not (list[RowsExpressionConditionGroup] | Unset): NOT groups of computed expression predicates.
            expression_array_contains (list[RowsExpressionArrayContainsPredicate] | Unset): Computed array-containment
                predicates.
            select (list[str] | Unset):
            json_extract (list[RowsJsonExtractProjection] | Unset):
            array_length (list[RowsArrayLengthProjection] | Unset):
            coalesce (list[RowsCoalesceProjection] | Unset):
            field_aliases (list[RowsFieldAliasProjection] | Unset):
            expressions (list[RowsExpressionProjection] | Unset): Typed row-expression projections.
            distinct_on (list[str] | Unset): Ordered field keys used to keep the first row per key after order_by and before
                pagination. The leading order_by fields must match. Field-only shorthand for `distinct_on_expressions`.
            distinct_on_expressions (list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset):
                Ordered typed row-expression keys used to keep the first row per computed key after order_by and before
                pagination. The leading order_by expression keys must match.
            order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
            limit (int | Unset):
            offset (int | Unset):
            total_mode (RowsQueryRequestTotalMode | Unset): Controls total counting for paged reads. `exact` scans all
                matches
                and returns an exact total. `bounded` may stop after the requested
                page and report a lower-bound total. `none` may omit work needed
                only for totals.
            row_claim (RowsRowClaim | Unset): Lockable base-row claim metadata. Public row-plan endpoints reject this
                field; it is only accepted by `rows/mutation-source` lockable base-row
                sources and internal/coordinator execution paths. `transaction_id` is
                the canonical field name.
            doc_key_range (Any | Unset): Physical row-key range selector used by routed typed row plans after
                durable range ownership is known. At least one of `start` or `end` must be present,
                and a bounded range must have `start < end`.
    """

    source_cte: str | Unset = UNSET
    where: Any | RowsWhereType0 | Unset = UNSET
    expression_where: list[RowsExpressionCondition] | Unset = UNSET
    expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
    expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
    select: list[str] | Unset = UNSET
    json_extract: list[RowsJsonExtractProjection] | Unset = UNSET
    array_length: list[RowsArrayLengthProjection] | Unset = UNSET
    coalesce: list[RowsCoalesceProjection] | Unset = UNSET
    field_aliases: list[RowsFieldAliasProjection] | Unset = UNSET
    expressions: list[RowsExpressionProjection] | Unset = UNSET
    distinct_on: list[str] | Unset = UNSET
    distinct_on_expressions: list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset = UNSET
    order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    limit: int | Unset = UNSET
    offset: int | Unset = UNSET
    total_mode: RowsQueryRequestTotalMode | Unset = UNSET
    row_claim: RowsRowClaim | Unset = UNSET
    doc_key_range: Any | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_where_type_0 import RowsWhereType0

        source_cte = self.source_cte

        where: Any | dict[str, Any] | Unset
        if isinstance(self.where, Unset):
            where = UNSET
        elif isinstance(self.where, RowsWhereType0):
            where = self.where.to_dict()
        else:
            where = self.where

        expression_where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expression_where, Unset):
            expression_where = []
            for expression_where_item_data in self.expression_where:
                expression_where_item = expression_where_item_data.to_dict()
                expression_where.append(expression_where_item)

        expression_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expression_any, Unset):
            expression_any = []
            for expression_any_item_data in self.expression_any:
                expression_any_item = expression_any_item_data.to_dict()
                expression_any.append(expression_any_item)

        expression_not: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expression_not, Unset):
            expression_not = []
            for expression_not_item_data in self.expression_not:
                expression_not_item = expression_not_item_data.to_dict()
                expression_not.append(expression_not_item)

        expression_array_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expression_array_contains, Unset):
            expression_array_contains = []
            for expression_array_contains_item_data in self.expression_array_contains:
                expression_array_contains_item = expression_array_contains_item_data.to_dict()
                expression_array_contains.append(expression_array_contains_item)

        select: list[str] | Unset = UNSET
        if not isinstance(self.select, Unset):
            select = self.select

        json_extract: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.json_extract, Unset):
            json_extract = []
            for json_extract_item_data in self.json_extract:
                json_extract_item = json_extract_item_data.to_dict()
                json_extract.append(json_extract_item)

        array_length: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.array_length, Unset):
            array_length = []
            for array_length_item_data in self.array_length:
                array_length_item = array_length_item_data.to_dict()
                array_length.append(array_length_item)

        coalesce: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.coalesce, Unset):
            coalesce = []
            for coalesce_item_data in self.coalesce:
                coalesce_item = coalesce_item_data.to_dict()
                coalesce.append(coalesce_item)

        field_aliases: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.field_aliases, Unset):
            field_aliases = []
            for field_aliases_item_data in self.field_aliases:
                field_aliases_item = field_aliases_item_data.to_dict()
                field_aliases.append(field_aliases_item)

        expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.expressions, Unset):
            expressions = []
            for expressions_item_data in self.expressions:
                expressions_item = expressions_item_data.to_dict()
                expressions.append(expressions_item)

        distinct_on: list[str] | Unset = UNSET
        if not isinstance(self.distinct_on, Unset):
            distinct_on = self.distinct_on

        distinct_on_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.distinct_on_expressions, Unset):
            distinct_on_expressions = []
            for distinct_on_expressions_item_data in self.distinct_on_expressions:
                distinct_on_expressions_item: dict[str, Any]
                if isinstance(distinct_on_expressions_item_data, RowsExpressionField):
                    distinct_on_expressions_item = distinct_on_expressions_item_data.to_dict()
                elif isinstance(distinct_on_expressions_item_data, RowsExpressionValue):
                    distinct_on_expressions_item = distinct_on_expressions_item_data.to_dict()
                else:
                    distinct_on_expressions_item = distinct_on_expressions_item_data.to_dict()

                distinct_on_expressions.append(distinct_on_expressions_item)

        order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.order_by, Unset):
            order_by = []
            for order_by_item_data in self.order_by:
                order_by_item: dict[str, Any]
                if isinstance(order_by_item_data, RowsQueryOrderField):
                    order_by_item = order_by_item_data.to_dict()
                else:
                    order_by_item = order_by_item_data.to_dict()

                order_by.append(order_by_item)

        limit = self.limit

        offset = self.offset

        total_mode: str | Unset = UNSET
        if not isinstance(self.total_mode, Unset):
            total_mode = self.total_mode.value

        row_claim: dict[str, Any] | Unset = UNSET
        if not isinstance(self.row_claim, Unset):
            row_claim = self.row_claim.to_dict()

        doc_key_range: Any | Unset
        if isinstance(self.doc_key_range, Unset):
            doc_key_range = UNSET
        else:
            doc_key_range = self.doc_key_range

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if source_cte is not UNSET:
            field_dict["source_cte"] = source_cte
        if where is not UNSET:
            field_dict["where"] = where
        if expression_where is not UNSET:
            field_dict["expression_where"] = expression_where
        if expression_any is not UNSET:
            field_dict["expression_any"] = expression_any
        if expression_not is not UNSET:
            field_dict["expression_not"] = expression_not
        if expression_array_contains is not UNSET:
            field_dict["expression_array_contains"] = expression_array_contains
        if select is not UNSET:
            field_dict["select"] = select
        if json_extract is not UNSET:
            field_dict["json_extract"] = json_extract
        if array_length is not UNSET:
            field_dict["array_length"] = array_length
        if coalesce is not UNSET:
            field_dict["coalesce"] = coalesce
        if field_aliases is not UNSET:
            field_dict["field_aliases"] = field_aliases
        if expressions is not UNSET:
            field_dict["expressions"] = expressions
        if distinct_on is not UNSET:
            field_dict["distinct_on"] = distinct_on
        if distinct_on_expressions is not UNSET:
            field_dict["distinct_on_expressions"] = distinct_on_expressions
        if order_by is not UNSET:
            field_dict["order_by"] = order_by
        if limit is not UNSET:
            field_dict["limit"] = limit
        if offset is not UNSET:
            field_dict["offset"] = offset
        if total_mode is not UNSET:
            field_dict["total_mode"] = total_mode
        if row_claim is not UNSET:
            field_dict["row_claim"] = row_claim
        if doc_key_range is not UNSET:
            field_dict["doc_key_range"] = doc_key_range

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_array_length_projection import RowsArrayLengthProjection
        from ..models.rows_coalesce_projection import RowsCoalesceProjection
        from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_projection import RowsExpressionProjection
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_field_alias_projection import RowsFieldAliasProjection
        from ..models.rows_json_extract_projection import RowsJsonExtractProjection
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_row_claim import RowsRowClaim
        from ..models.rows_where_type_0 import RowsWhereType0

        d = dict(src_dict)
        source_cte = d.pop("source_cte", UNSET)

        def _parse_where(data: object) -> Any | RowsWhereType0 | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_rows_where_type_0 = RowsWhereType0.from_dict(data)

                return componentsschemas_rows_where_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(Any | RowsWhereType0 | Unset, data)

        where = _parse_where(d.pop("where", UNSET))

        _expression_where = d.pop("expression_where", UNSET)
        expression_where: list[RowsExpressionCondition] | Unset = UNSET
        if _expression_where is not UNSET:
            expression_where = []
            for expression_where_item_data in _expression_where:
                expression_where_item = RowsExpressionCondition.from_dict(expression_where_item_data)

                expression_where.append(expression_where_item)

        _expression_any = d.pop("expression_any", UNSET)
        expression_any: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _expression_any is not UNSET:
            expression_any = []
            for expression_any_item_data in _expression_any:
                expression_any_item = RowsExpressionConditionGroup.from_dict(expression_any_item_data)

                expression_any.append(expression_any_item)

        _expression_not = d.pop("expression_not", UNSET)
        expression_not: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _expression_not is not UNSET:
            expression_not = []
            for expression_not_item_data in _expression_not:
                expression_not_item = RowsExpressionConditionGroup.from_dict(expression_not_item_data)

                expression_not.append(expression_not_item)

        _expression_array_contains = d.pop("expression_array_contains", UNSET)
        expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
        if _expression_array_contains is not UNSET:
            expression_array_contains = []
            for expression_array_contains_item_data in _expression_array_contains:
                expression_array_contains_item = RowsExpressionArrayContainsPredicate.from_dict(
                    expression_array_contains_item_data
                )

                expression_array_contains.append(expression_array_contains_item)

        select = cast(list[str], d.pop("select", UNSET))

        _json_extract = d.pop("json_extract", UNSET)
        json_extract: list[RowsJsonExtractProjection] | Unset = UNSET
        if _json_extract is not UNSET:
            json_extract = []
            for json_extract_item_data in _json_extract:
                json_extract_item = RowsJsonExtractProjection.from_dict(json_extract_item_data)

                json_extract.append(json_extract_item)

        _array_length = d.pop("array_length", UNSET)
        array_length: list[RowsArrayLengthProjection] | Unset = UNSET
        if _array_length is not UNSET:
            array_length = []
            for array_length_item_data in _array_length:
                array_length_item = RowsArrayLengthProjection.from_dict(array_length_item_data)

                array_length.append(array_length_item)

        _coalesce = d.pop("coalesce", UNSET)
        coalesce: list[RowsCoalesceProjection] | Unset = UNSET
        if _coalesce is not UNSET:
            coalesce = []
            for coalesce_item_data in _coalesce:
                coalesce_item = RowsCoalesceProjection.from_dict(coalesce_item_data)

                coalesce.append(coalesce_item)

        _field_aliases = d.pop("field_aliases", UNSET)
        field_aliases: list[RowsFieldAliasProjection] | Unset = UNSET
        if _field_aliases is not UNSET:
            field_aliases = []
            for field_aliases_item_data in _field_aliases:
                field_aliases_item = RowsFieldAliasProjection.from_dict(field_aliases_item_data)

                field_aliases.append(field_aliases_item)

        _expressions = d.pop("expressions", UNSET)
        expressions: list[RowsExpressionProjection] | Unset = UNSET
        if _expressions is not UNSET:
            expressions = []
            for expressions_item_data in _expressions:
                expressions_item = RowsExpressionProjection.from_dict(expressions_item_data)

                expressions.append(expressions_item)

        distinct_on = cast(list[str], d.pop("distinct_on", UNSET))

        _distinct_on_expressions = d.pop("distinct_on_expressions", UNSET)
        distinct_on_expressions: list[RowsExpressionField | RowsExpressionOperator | RowsExpressionValue] | Unset = (
            UNSET
        )
        if _distinct_on_expressions is not UNSET:
            distinct_on_expressions = []
            for distinct_on_expressions_item_data in _distinct_on_expressions:

                def _parse_distinct_on_expressions_item(
                    data: object,
                ) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue:
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_rows_expression_type_0 = RowsExpressionField.from_dict(data)

                        return componentsschemas_rows_expression_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_rows_expression_type_1 = RowsExpressionValue.from_dict(data)

                        return componentsschemas_rows_expression_type_1
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_expression_type_2 = RowsExpressionOperator.from_dict(data)

                    return componentsschemas_rows_expression_type_2

                distinct_on_expressions_item = _parse_distinct_on_expressions_item(distinct_on_expressions_item_data)

                distinct_on_expressions.append(distinct_on_expressions_item)

        _order_by = d.pop("order_by", UNSET)
        order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
        if _order_by is not UNSET:
            order_by = []
            for order_by_item_data in _order_by:

                def _parse_order_by_item(data: object) -> RowsQueryOrderExpression | RowsQueryOrderField:
                    try:
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_rows_query_order_type_0 = RowsQueryOrderField.from_dict(data)

                        return componentsschemas_rows_query_order_type_0
                    except (TypeError, ValueError, AttributeError, KeyError):
                        pass
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_rows_query_order_type_1 = RowsQueryOrderExpression.from_dict(data)

                    return componentsschemas_rows_query_order_type_1

                order_by_item = _parse_order_by_item(order_by_item_data)

                order_by.append(order_by_item)

        limit = d.pop("limit", UNSET)

        offset = d.pop("offset", UNSET)

        _total_mode = d.pop("total_mode", UNSET)
        total_mode: RowsQueryRequestTotalMode | Unset
        if isinstance(_total_mode, Unset):
            total_mode = UNSET
        else:
            total_mode = RowsQueryRequestTotalMode(_total_mode)

        _row_claim = d.pop("row_claim", UNSET)
        row_claim: RowsRowClaim | Unset
        if isinstance(_row_claim, Unset):
            row_claim = UNSET
        else:
            row_claim = RowsRowClaim.from_dict(_row_claim)

        def _parse_doc_key_range(data: object) -> Any | Unset:
            if isinstance(data, Unset):
                return data
            return cast(Any | Unset, data)

        doc_key_range = _parse_doc_key_range(d.pop("doc_key_range", UNSET))

        rows_query_request = cls(
            source_cte=source_cte,
            where=where,
            expression_where=expression_where,
            expression_any=expression_any,
            expression_not=expression_not,
            expression_array_contains=expression_array_contains,
            select=select,
            json_extract=json_extract,
            array_length=array_length,
            coalesce=coalesce,
            field_aliases=field_aliases,
            expressions=expressions,
            distinct_on=distinct_on,
            distinct_on_expressions=distinct_on_expressions,
            order_by=order_by,
            limit=limit,
            offset=offset,
            total_mode=total_mode,
            row_claim=row_claim,
            doc_key_range=doc_key_range,
        )

        return rows_query_request
