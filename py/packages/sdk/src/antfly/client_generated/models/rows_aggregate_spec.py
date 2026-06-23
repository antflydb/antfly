from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.rows_aggregate_spec_op import RowsAggregateSpecOp
from ..models.rows_aggregate_spec_percentile_order import RowsAggregateSpecPercentileOrder
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
    from ..models.rows_expression_field import RowsExpressionField
    from ..models.rows_expression_operator import RowsExpressionOperator
    from ..models.rows_expression_value import RowsExpressionValue
    from ..models.rows_query_order_expression import RowsQueryOrderExpression
    from ..models.rows_query_order_field import RowsQueryOrderField
    from ..models.rows_where_atom import RowsWhereAtom
    from ..models.rows_where_type_0 import RowsWhereType0


T = TypeVar("T", bound="RowsAggregateSpec")


@_attrs_define
class RowsAggregateSpec:
    """
    Attributes:
        name (str):
        op (RowsAggregateSpecOp):
        field (str | Unset):
        expr (RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset): Shared typed row-expression
            AST. A node is exactly one of `{ "field": "name" }`,
            `{ "value": ... }`, or an operator node such as
            `{ "op": "lower", "args": [{ "field": "email" }] }`. Supported
            operators are the shared row-local expression surface used by schema
            predicates, mutation expressions, query projections, filters, grouping,
            ordering, and SQL lowering.
            Mutation expressions may set `source` to `existing` or `proposed`; query
            expressions use the default row source.
        distinct (bool | Unset):
        distinct_max_items (int | Unset):
        percentile (float | Unset): Fraction for percentile_cont and percentile_disc aggregate specs.
        percentiles (list[float] | Unset): Fractions for array-valued percentile_cont and percentile_disc aggregate
            specs.
        percentile_max_items (int | Unset): Maximum bounded per-group sample count for percentile_cont and
            percentile_disc.
        percentile_order (RowsAggregateSpecPercentileOrder | Unset): Ordered-set sample direction for percentile_cont
            and percentile_disc; deterministic tie-break direction for mode.
        array_max_items (int | Unset):
        array_order_by (list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset):
        delimiter (str | Unset): Delimiter for string_agg aggregate specs.
        filter_ (Any | RowsWhereType0 | Unset): Canonical row predicate tree. A top-level `where` is one predicate
            atom, an `all` conjunction of atoms, `any` / `not` branch groups, or an
            `all` conjunction plus branch groups. Branches may contain scalar,
            membership, array, JSON, and text-pattern atoms; the server stores
            branches containing structured atoms in native mixed access predicate
            groups and keeps scalar-only branches in scalar predicate groups.
        filter_array_any (list[RowsWhereAtom] | Unset): Conjunctive declared-array element-match filters for this
            aggregate. Each item must use `op: array_any`.
        filter_array_contains (list[RowsWhereAtom] | Unset): Conjunctive declared-array containment filters for this
            aggregate. Each item must use `op: array_contains`.
        filter_array_eq (list[RowsWhereAtom] | Unset): Conjunctive declared-array equality filters for this aggregate.
            Each item must use `op: array_eq`.
        filter_in (list[RowsWhereAtom] | Unset): Conjunctive scalar membership filters for this aggregate. Each item
            must use `op: in` or `op: not_in`.
        filter_json_contains (list[RowsWhereAtom] | Unset): Conjunctive declared-JSON containment filters for this
            aggregate. Each item must use `op: json_contains`.
        filter_json_path_eq (list[RowsWhereAtom] | Unset): Conjunctive declared-JSON path equality filters for this
            aggregate. Each item must use `op: json_path_eq`.
        filter_json_path_exists (list[RowsWhereAtom] | Unset): Conjunctive declared-JSON path-existence filters for this
            aggregate. Each item must use `op: json_path_exists`.
        filter_text_patterns (list[RowsWhereAtom] | Unset): Conjunctive text-pattern filters for this aggregate. Each
            item must use `op: text_pattern`.
        filter_expressions (list[RowsExpressionCondition] | Unset):
        filter_expression_array_contains (list[RowsExpressionArrayContainsPredicate] | Unset): Conjunctive computed
            array-containment filters for this aggregate.
        filter_any (list[RowsExpressionConditionGroup] | Unset): Disjunction of input-row expression predicate groups
            for this aggregate. Each group is a conjunction; the aggregate consumes a row when at least one group matches.
        filter_not (list[RowsExpressionConditionGroup] | Unset): Negated input-row expression predicate groups for this
            aggregate. The aggregate skips a row when any group matches.
    """

    name: str
    op: RowsAggregateSpecOp
    field: str | Unset = UNSET
    expr: RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset = UNSET
    distinct: bool | Unset = UNSET
    distinct_max_items: int | Unset = UNSET
    percentile: float | Unset = UNSET
    percentiles: list[float] | Unset = UNSET
    percentile_max_items: int | Unset = UNSET
    percentile_order: RowsAggregateSpecPercentileOrder | Unset = UNSET
    array_max_items: int | Unset = UNSET
    array_order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
    delimiter: str | Unset = UNSET
    filter_: Any | RowsWhereType0 | Unset = UNSET
    filter_array_any: list[RowsWhereAtom] | Unset = UNSET
    filter_array_contains: list[RowsWhereAtom] | Unset = UNSET
    filter_array_eq: list[RowsWhereAtom] | Unset = UNSET
    filter_in: list[RowsWhereAtom] | Unset = UNSET
    filter_json_contains: list[RowsWhereAtom] | Unset = UNSET
    filter_json_path_eq: list[RowsWhereAtom] | Unset = UNSET
    filter_json_path_exists: list[RowsWhereAtom] | Unset = UNSET
    filter_text_patterns: list[RowsWhereAtom] | Unset = UNSET
    filter_expressions: list[RowsExpressionCondition] | Unset = UNSET
    filter_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
    filter_any: list[RowsExpressionConditionGroup] | Unset = UNSET
    filter_not: list[RowsExpressionConditionGroup] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_where_type_0 import RowsWhereType0

        name = self.name

        op = self.op.value

        field = self.field

        expr: dict[str, Any] | Unset
        if isinstance(self.expr, Unset):
            expr = UNSET
        elif isinstance(self.expr, RowsExpressionField):
            expr = self.expr.to_dict()
        elif isinstance(self.expr, RowsExpressionValue):
            expr = self.expr.to_dict()
        else:
            expr = self.expr.to_dict()

        distinct = self.distinct

        distinct_max_items = self.distinct_max_items

        percentile = self.percentile

        percentiles: list[float] | Unset = UNSET
        if not isinstance(self.percentiles, Unset):
            percentiles = self.percentiles

        percentile_max_items = self.percentile_max_items

        percentile_order: str | Unset = UNSET
        if not isinstance(self.percentile_order, Unset):
            percentile_order = self.percentile_order.value

        array_max_items = self.array_max_items

        array_order_by: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.array_order_by, Unset):
            array_order_by = []
            for array_order_by_item_data in self.array_order_by:
                array_order_by_item: dict[str, Any]
                if isinstance(array_order_by_item_data, RowsQueryOrderField):
                    array_order_by_item = array_order_by_item_data.to_dict()
                else:
                    array_order_by_item = array_order_by_item_data.to_dict()

                array_order_by.append(array_order_by_item)

        delimiter = self.delimiter

        filter_: Any | dict[str, Any] | Unset
        if isinstance(self.filter_, Unset):
            filter_ = UNSET
        elif isinstance(self.filter_, RowsWhereType0):
            filter_ = self.filter_.to_dict()
        else:
            filter_ = self.filter_

        filter_array_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_array_any, Unset):
            filter_array_any = []
            for filter_array_any_item_data in self.filter_array_any:
                filter_array_any_item = filter_array_any_item_data.to_dict()
                filter_array_any.append(filter_array_any_item)

        filter_array_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_array_contains, Unset):
            filter_array_contains = []
            for filter_array_contains_item_data in self.filter_array_contains:
                filter_array_contains_item = filter_array_contains_item_data.to_dict()
                filter_array_contains.append(filter_array_contains_item)

        filter_array_eq: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_array_eq, Unset):
            filter_array_eq = []
            for filter_array_eq_item_data in self.filter_array_eq:
                filter_array_eq_item = filter_array_eq_item_data.to_dict()
                filter_array_eq.append(filter_array_eq_item)

        filter_in: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_in, Unset):
            filter_in = []
            for filter_in_item_data in self.filter_in:
                filter_in_item = filter_in_item_data.to_dict()
                filter_in.append(filter_in_item)

        filter_json_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_json_contains, Unset):
            filter_json_contains = []
            for filter_json_contains_item_data in self.filter_json_contains:
                filter_json_contains_item = filter_json_contains_item_data.to_dict()
                filter_json_contains.append(filter_json_contains_item)

        filter_json_path_eq: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_json_path_eq, Unset):
            filter_json_path_eq = []
            for filter_json_path_eq_item_data in self.filter_json_path_eq:
                filter_json_path_eq_item = filter_json_path_eq_item_data.to_dict()
                filter_json_path_eq.append(filter_json_path_eq_item)

        filter_json_path_exists: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_json_path_exists, Unset):
            filter_json_path_exists = []
            for filter_json_path_exists_item_data in self.filter_json_path_exists:
                filter_json_path_exists_item = filter_json_path_exists_item_data.to_dict()
                filter_json_path_exists.append(filter_json_path_exists_item)

        filter_text_patterns: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_text_patterns, Unset):
            filter_text_patterns = []
            for filter_text_patterns_item_data in self.filter_text_patterns:
                filter_text_patterns_item = filter_text_patterns_item_data.to_dict()
                filter_text_patterns.append(filter_text_patterns_item)

        filter_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_expressions, Unset):
            filter_expressions = []
            for filter_expressions_item_data in self.filter_expressions:
                filter_expressions_item = filter_expressions_item_data.to_dict()
                filter_expressions.append(filter_expressions_item)

        filter_expression_array_contains: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_expression_array_contains, Unset):
            filter_expression_array_contains = []
            for filter_expression_array_contains_item_data in self.filter_expression_array_contains:
                filter_expression_array_contains_item = filter_expression_array_contains_item_data.to_dict()
                filter_expression_array_contains.append(filter_expression_array_contains_item)

        filter_any: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_any, Unset):
            filter_any = []
            for filter_any_item_data in self.filter_any:
                filter_any_item = filter_any_item_data.to_dict()
                filter_any.append(filter_any_item)

        filter_not: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.filter_not, Unset):
            filter_not = []
            for filter_not_item_data in self.filter_not:
                filter_not_item = filter_not_item_data.to_dict()
                filter_not.append(filter_not_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "op": op,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if expr is not UNSET:
            field_dict["expr"] = expr
        if distinct is not UNSET:
            field_dict["distinct"] = distinct
        if distinct_max_items is not UNSET:
            field_dict["distinct_max_items"] = distinct_max_items
        if percentile is not UNSET:
            field_dict["percentile"] = percentile
        if percentiles is not UNSET:
            field_dict["percentiles"] = percentiles
        if percentile_max_items is not UNSET:
            field_dict["percentile_max_items"] = percentile_max_items
        if percentile_order is not UNSET:
            field_dict["percentile_order"] = percentile_order
        if array_max_items is not UNSET:
            field_dict["array_max_items"] = array_max_items
        if array_order_by is not UNSET:
            field_dict["array_order_by"] = array_order_by
        if delimiter is not UNSET:
            field_dict["delimiter"] = delimiter
        if filter_ is not UNSET:
            field_dict["filter"] = filter_
        if filter_array_any is not UNSET:
            field_dict["filter_array_any"] = filter_array_any
        if filter_array_contains is not UNSET:
            field_dict["filter_array_contains"] = filter_array_contains
        if filter_array_eq is not UNSET:
            field_dict["filter_array_eq"] = filter_array_eq
        if filter_in is not UNSET:
            field_dict["filter_in"] = filter_in
        if filter_json_contains is not UNSET:
            field_dict["filter_json_contains"] = filter_json_contains
        if filter_json_path_eq is not UNSET:
            field_dict["filter_json_path_eq"] = filter_json_path_eq
        if filter_json_path_exists is not UNSET:
            field_dict["filter_json_path_exists"] = filter_json_path_exists
        if filter_text_patterns is not UNSET:
            field_dict["filter_text_patterns"] = filter_text_patterns
        if filter_expressions is not UNSET:
            field_dict["filter_expressions"] = filter_expressions
        if filter_expression_array_contains is not UNSET:
            field_dict["filter_expression_array_contains"] = filter_expression_array_contains
        if filter_any is not UNSET:
            field_dict["filter_any"] = filter_any
        if filter_not is not UNSET:
            field_dict["filter_not"] = filter_not

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.rows_expression_array_contains_predicate import RowsExpressionArrayContainsPredicate
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_expression_condition_group import RowsExpressionConditionGroup
        from ..models.rows_expression_field import RowsExpressionField
        from ..models.rows_expression_operator import RowsExpressionOperator
        from ..models.rows_expression_value import RowsExpressionValue
        from ..models.rows_query_order_expression import RowsQueryOrderExpression
        from ..models.rows_query_order_field import RowsQueryOrderField
        from ..models.rows_where_atom import RowsWhereAtom
        from ..models.rows_where_type_0 import RowsWhereType0

        d = dict(src_dict)
        name = d.pop("name")

        op = RowsAggregateSpecOp(d.pop("op"))

        field = d.pop("field", UNSET)

        def _parse_expr(data: object) -> RowsExpressionField | RowsExpressionOperator | RowsExpressionValue | Unset:
            if isinstance(data, Unset):
                return data
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

        expr = _parse_expr(d.pop("expr", UNSET))

        distinct = d.pop("distinct", UNSET)

        distinct_max_items = d.pop("distinct_max_items", UNSET)

        percentile = d.pop("percentile", UNSET)

        percentiles = cast(list[float], d.pop("percentiles", UNSET))

        percentile_max_items = d.pop("percentile_max_items", UNSET)

        _percentile_order = d.pop("percentile_order", UNSET)
        percentile_order: RowsAggregateSpecPercentileOrder | Unset
        if isinstance(_percentile_order, Unset):
            percentile_order = UNSET
        else:
            percentile_order = RowsAggregateSpecPercentileOrder(_percentile_order)

        array_max_items = d.pop("array_max_items", UNSET)

        _array_order_by = d.pop("array_order_by", UNSET)
        array_order_by: list[RowsQueryOrderExpression | RowsQueryOrderField] | Unset = UNSET
        if _array_order_by is not UNSET:
            array_order_by = []
            for array_order_by_item_data in _array_order_by:

                def _parse_array_order_by_item(data: object) -> RowsQueryOrderExpression | RowsQueryOrderField:
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

                array_order_by_item = _parse_array_order_by_item(array_order_by_item_data)

                array_order_by.append(array_order_by_item)

        delimiter = d.pop("delimiter", UNSET)

        def _parse_filter_(data: object) -> Any | RowsWhereType0 | Unset:
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

        filter_ = _parse_filter_(d.pop("filter", UNSET))

        _filter_array_any = d.pop("filter_array_any", UNSET)
        filter_array_any: list[RowsWhereAtom] | Unset = UNSET
        if _filter_array_any is not UNSET:
            filter_array_any = []
            for filter_array_any_item_data in _filter_array_any:
                filter_array_any_item = RowsWhereAtom.from_dict(filter_array_any_item_data)

                filter_array_any.append(filter_array_any_item)

        _filter_array_contains = d.pop("filter_array_contains", UNSET)
        filter_array_contains: list[RowsWhereAtom] | Unset = UNSET
        if _filter_array_contains is not UNSET:
            filter_array_contains = []
            for filter_array_contains_item_data in _filter_array_contains:
                filter_array_contains_item = RowsWhereAtom.from_dict(filter_array_contains_item_data)

                filter_array_contains.append(filter_array_contains_item)

        _filter_array_eq = d.pop("filter_array_eq", UNSET)
        filter_array_eq: list[RowsWhereAtom] | Unset = UNSET
        if _filter_array_eq is not UNSET:
            filter_array_eq = []
            for filter_array_eq_item_data in _filter_array_eq:
                filter_array_eq_item = RowsWhereAtom.from_dict(filter_array_eq_item_data)

                filter_array_eq.append(filter_array_eq_item)

        _filter_in = d.pop("filter_in", UNSET)
        filter_in: list[RowsWhereAtom] | Unset = UNSET
        if _filter_in is not UNSET:
            filter_in = []
            for filter_in_item_data in _filter_in:
                filter_in_item = RowsWhereAtom.from_dict(filter_in_item_data)

                filter_in.append(filter_in_item)

        _filter_json_contains = d.pop("filter_json_contains", UNSET)
        filter_json_contains: list[RowsWhereAtom] | Unset = UNSET
        if _filter_json_contains is not UNSET:
            filter_json_contains = []
            for filter_json_contains_item_data in _filter_json_contains:
                filter_json_contains_item = RowsWhereAtom.from_dict(filter_json_contains_item_data)

                filter_json_contains.append(filter_json_contains_item)

        _filter_json_path_eq = d.pop("filter_json_path_eq", UNSET)
        filter_json_path_eq: list[RowsWhereAtom] | Unset = UNSET
        if _filter_json_path_eq is not UNSET:
            filter_json_path_eq = []
            for filter_json_path_eq_item_data in _filter_json_path_eq:
                filter_json_path_eq_item = RowsWhereAtom.from_dict(filter_json_path_eq_item_data)

                filter_json_path_eq.append(filter_json_path_eq_item)

        _filter_json_path_exists = d.pop("filter_json_path_exists", UNSET)
        filter_json_path_exists: list[RowsWhereAtom] | Unset = UNSET
        if _filter_json_path_exists is not UNSET:
            filter_json_path_exists = []
            for filter_json_path_exists_item_data in _filter_json_path_exists:
                filter_json_path_exists_item = RowsWhereAtom.from_dict(filter_json_path_exists_item_data)

                filter_json_path_exists.append(filter_json_path_exists_item)

        _filter_text_patterns = d.pop("filter_text_patterns", UNSET)
        filter_text_patterns: list[RowsWhereAtom] | Unset = UNSET
        if _filter_text_patterns is not UNSET:
            filter_text_patterns = []
            for filter_text_patterns_item_data in _filter_text_patterns:
                filter_text_patterns_item = RowsWhereAtom.from_dict(filter_text_patterns_item_data)

                filter_text_patterns.append(filter_text_patterns_item)

        _filter_expressions = d.pop("filter_expressions", UNSET)
        filter_expressions: list[RowsExpressionCondition] | Unset = UNSET
        if _filter_expressions is not UNSET:
            filter_expressions = []
            for filter_expressions_item_data in _filter_expressions:
                filter_expressions_item = RowsExpressionCondition.from_dict(filter_expressions_item_data)

                filter_expressions.append(filter_expressions_item)

        _filter_expression_array_contains = d.pop("filter_expression_array_contains", UNSET)
        filter_expression_array_contains: list[RowsExpressionArrayContainsPredicate] | Unset = UNSET
        if _filter_expression_array_contains is not UNSET:
            filter_expression_array_contains = []
            for filter_expression_array_contains_item_data in _filter_expression_array_contains:
                filter_expression_array_contains_item = RowsExpressionArrayContainsPredicate.from_dict(
                    filter_expression_array_contains_item_data
                )

                filter_expression_array_contains.append(filter_expression_array_contains_item)

        _filter_any = d.pop("filter_any", UNSET)
        filter_any: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _filter_any is not UNSET:
            filter_any = []
            for filter_any_item_data in _filter_any:
                filter_any_item = RowsExpressionConditionGroup.from_dict(filter_any_item_data)

                filter_any.append(filter_any_item)

        _filter_not = d.pop("filter_not", UNSET)
        filter_not: list[RowsExpressionConditionGroup] | Unset = UNSET
        if _filter_not is not UNSET:
            filter_not = []
            for filter_not_item_data in _filter_not:
                filter_not_item = RowsExpressionConditionGroup.from_dict(filter_not_item_data)

                filter_not.append(filter_not_item)

        rows_aggregate_spec = cls(
            name=name,
            op=op,
            field=field,
            expr=expr,
            distinct=distinct,
            distinct_max_items=distinct_max_items,
            percentile=percentile,
            percentiles=percentiles,
            percentile_max_items=percentile_max_items,
            percentile_order=percentile_order,
            array_max_items=array_max_items,
            array_order_by=array_order_by,
            delimiter=delimiter,
            filter_=filter_,
            filter_array_any=filter_array_any,
            filter_array_contains=filter_array_contains,
            filter_array_eq=filter_array_eq,
            filter_in=filter_in,
            filter_json_contains=filter_json_contains,
            filter_json_path_eq=filter_json_path_eq,
            filter_json_path_exists=filter_json_path_exists,
            filter_text_patterns=filter_text_patterns,
            filter_expressions=filter_expressions,
            filter_expression_array_contains=filter_expression_array_contains,
            filter_any=filter_any,
            filter_not=filter_not,
        )

        return rows_aggregate_spec
