from antfly import RelationalColumnExpression, RelationalIndexPredicate
from antfly.client_generated.models.relational_expression_op import RelationalExpressionOp
from antfly.client_generated.models.relational_expression_type import RelationalExpressionType
from antfly.client_generated.models.sql_builtin_type import SQLBuiltinType


def test_generated_array_constructor_preserves_identity_children_and_typed_null():
    source = {
        "column": "a",
        "expression": {
            "op": "array",
            "sql_type": "int32",
            "args": [
                {"op": "literal", "type": "integer", "sql_type": "int32", "value": 1},
                {"op": "literal", "type": "integer", "sql_type": "int32", "value": None},
            ],
        },
    }
    model = RelationalColumnExpression.from_dict(source)
    assert model.expression.op is RelationalExpressionOp.ARRAY
    assert model.expression.sql_type is SQLBuiltinType.INT32
    assert model.to_dict() == source


def test_generated_array_literal_keeps_exact_cells_bounds_and_sql_nulls():
    for identity in SQLBuiltinType:
        for value in (
            None,
            {
                "dimensions": [{"length": 2, "lower_bound": -4}],
                "values": ["9007199254740993", None],
                "sql_nulls": [False, True],
            },
        ):
            source = {
                "column": "a",
                "expression": {
                    "op": "literal",
                    "type": "sql_array",
                    "sql_type": identity.value,
                    "value": value,
                },
            }
            model = RelationalColumnExpression.from_dict(source)
            assert model.expression.type_ is RelationalExpressionType.SQL_ARRAY
            assert model.expression.sql_type is identity
            assert model.to_dict() == source


def test_generated_recursive_expression_and_partial_predicate_roundtrip():
    value = {
        "column": "total",
        "expression": {
            "op": "coalesce",
            "args": [
                {"op": "literal", "type": "integer", "value": None},
                {"op": "literal", "type": "integer", "value": "9007199254740993"},
            ],
        },
    }
    assert RelationalColumnExpression.from_dict(value).to_dict() == value
    predicate = {"column": "total", "op": "eq", "value": "9007199254740993"}
    assert RelationalIndexPredicate.from_dict(predicate).to_dict() == predicate


def test_generated_numeric_assignment_cast_preserves_builtin_identity():
    value = {
        "column": "n",
        "expression": {
            "op": "cast",
            "type": "integer",
            "sql_type": "int16",
            "args": [{"op": "literal", "type": "integer", "sql_type": "int32", "value": 32768}],
        },
    }
    model = RelationalColumnExpression.from_dict(value)
    assert model.expression.op is RelationalExpressionOp.CAST
    assert model.expression.sql_type is SQLBuiltinType.INT16
    assert model.expression.args[0].sql_type is SQLBuiltinType.INT32
    assert model.to_dict() == value


def test_generated_conditional_expression_keeps_order_and_null_fallback():
    value = {
        "column": "n",
        "expression": {
            "op": "case_when",
            "args": [
                {"op": "literal", "type": "boolean", "value": True},
                {"op": "column", "column": "source"},
                {"op": "literal", "type": "integer", "sql_type": "int32", "value": None},
            ],
        },
    }
    model = RelationalColumnExpression.from_dict(value)
    assert model.expression.op is RelationalExpressionOp.CASE_WHEN
    assert model.to_dict() == value
