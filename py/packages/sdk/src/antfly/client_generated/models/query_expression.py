from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.query_expression_call import QueryExpressionCall
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.query_expression_criteria_type_0 import QueryExpressionCriteriaType0
    from ..models.query_expression_questions_item import QueryExpressionQuestionsItem


T = TypeVar("T", bound="QueryExpression")


@_attrs_define
class QueryExpression:
    """Exactly one of literal, field, ref, or call. A call requires input and
    decider. ai_decide requires questions; ai_probability requires statement;
    ai_choice and ai_score require instructions and criteria. Named refs may
    select nested JSON members with dotted paths. Binding cycles are invalid.

        Attributes:
            literal (Any | Unset):
            field (str | Unset):
            ref (str | Unset):
            call (QueryExpressionCall | Unset):
            input_ (QueryExpression | Unset): Exactly one of literal, field, ref, or call. A call requires input and
                decider. ai_decide requires questions; ai_probability requires statement;
                ai_choice and ai_score require instructions and criteria. Named refs may
                select nested JSON members with dotted paths. Binding cycles are invalid.
            decider (str | Unset):
            questions (list[QueryExpressionQuestionsItem] | Unset): Named decision question array using choice choices,
                score levels, or predicate instructions.
            statement (str | Unset):
            instructions (str | Unset):
            criteria (list[str] | QueryExpressionCriteriaType0 | Unset): Choice ID map or ordered score level array.
    """

    literal: Any | Unset = UNSET
    field: str | Unset = UNSET
    ref: str | Unset = UNSET
    call: QueryExpressionCall | Unset = UNSET
    input_: QueryExpression | Unset = UNSET
    decider: str | Unset = UNSET
    questions: list[QueryExpressionQuestionsItem] | Unset = UNSET
    statement: str | Unset = UNSET
    instructions: str | Unset = UNSET
    criteria: list[str] | QueryExpressionCriteriaType0 | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.query_expression_criteria_type_0 import QueryExpressionCriteriaType0

        literal = self.literal

        field = self.field

        ref = self.ref

        call: str | Unset = UNSET
        if not isinstance(self.call, Unset):
            call = self.call.value

        input_: dict[str, Any] | Unset = UNSET
        if not isinstance(self.input_, Unset):
            input_ = self.input_.to_dict()

        decider = self.decider

        questions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.questions, Unset):
            questions = []
            for questions_item_data in self.questions:
                questions_item = questions_item_data.to_dict()
                questions.append(questions_item)

        statement = self.statement

        instructions = self.instructions

        criteria: dict[str, Any] | list[str] | Unset
        if isinstance(self.criteria, Unset):
            criteria = UNSET
        elif isinstance(self.criteria, QueryExpressionCriteriaType0):
            criteria = self.criteria.to_dict()
        else:
            criteria = self.criteria

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if literal is not UNSET:
            field_dict["literal"] = literal
        if field is not UNSET:
            field_dict["field"] = field
        if ref is not UNSET:
            field_dict["ref"] = ref
        if call is not UNSET:
            field_dict["call"] = call
        if input_ is not UNSET:
            field_dict["input"] = input_
        if decider is not UNSET:
            field_dict["decider"] = decider
        if questions is not UNSET:
            field_dict["questions"] = questions
        if statement is not UNSET:
            field_dict["statement"] = statement
        if instructions is not UNSET:
            field_dict["instructions"] = instructions
        if criteria is not UNSET:
            field_dict["criteria"] = criteria

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.query_expression_criteria_type_0 import QueryExpressionCriteriaType0
        from ..models.query_expression_questions_item import QueryExpressionQuestionsItem

        d = dict(src_dict)
        literal = d.pop("literal", UNSET)

        field = d.pop("field", UNSET)

        ref = d.pop("ref", UNSET)

        _call = d.pop("call", UNSET)
        call: QueryExpressionCall | Unset
        if isinstance(_call, Unset):
            call = UNSET
        else:
            call = QueryExpressionCall(_call)

        _input_ = d.pop("input", UNSET)
        input_: QueryExpression | Unset
        if isinstance(_input_, Unset):
            input_ = UNSET
        else:
            input_ = QueryExpression.from_dict(_input_)

        decider = d.pop("decider", UNSET)

        _questions = d.pop("questions", UNSET)
        questions: list[QueryExpressionQuestionsItem] | Unset = UNSET
        if _questions is not UNSET:
            questions = []
            for questions_item_data in _questions:
                questions_item = QueryExpressionQuestionsItem.from_dict(questions_item_data)

                questions.append(questions_item)

        statement = d.pop("statement", UNSET)

        instructions = d.pop("instructions", UNSET)

        def _parse_criteria(data: object) -> list[str] | QueryExpressionCriteriaType0 | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                criteria_type_0 = QueryExpressionCriteriaType0.from_dict(data)

                return criteria_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, list):
                raise TypeError()
            criteria_type_1 = cast(list[str], data)

            return criteria_type_1

        criteria = _parse_criteria(d.pop("criteria", UNSET))

        query_expression = cls(
            literal=literal,
            field=field,
            ref=ref,
            call=call,
            input_=input_,
            decider=decider,
            questions=questions,
            statement=statement,
            instructions=instructions,
            criteria=criteria,
        )

        return query_expression
