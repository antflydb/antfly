from antfly import RelationalColumnExpression, RelationalIndexPredicate


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
