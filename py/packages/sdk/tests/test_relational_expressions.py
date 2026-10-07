from antfly import RelationalColumnExpression, RelationalIndexPredicate
from antfly.client_generated.models.sql_builtin_type import SQLBuiltinType
from antfly.client_generated.models.relational_expression_op import RelationalExpressionOp


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
