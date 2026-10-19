from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_resolver_adjudication_config_on_failure import GraphResolverAdjudicationConfigOnFailure
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.choice_decision_question import ChoiceDecisionQuestion
    from ..models.decider_config import DeciderConfig
    from ..models.graph_resolver_adjudication_acceptance import GraphResolverAdjudicationAcceptance
    from ..models.graph_resolver_adjudication_config_description_inputs import (
        GraphResolverAdjudicationConfigDescriptionInputs,
    )


T = TypeVar("T", bound="GraphResolverAdjudicationConfig")


@_attrs_define
class GraphResolverAdjudicationConfig:
    """Adjudicate the best deterministic REVIEW candidate with a typed decision. MATCH and NO_MATCH bypass the model.
    Evidence is authoritative resolution data, retained independently of work caches and reused on recovery. Changes
    trigger versioned re-resolution. This resolves mentions, not global key merges.

        Attributes:
            decider (DeciderConfig):
            template (str): Bounded interpolation of m.text, m.surface_form, m.canonical_text, m.label, m.description,
                c.canonical_name, c.label and c.description. Missing descriptions render empty text, allowing name-only
                decisions.
            question (ChoiceDecisionQuestion):
            accept (GraphResolverAdjudicationAcceptance):
            reject (GraphResolverAdjudicationAcceptance | Unset):
            on_failure (GraphResolverAdjudicationConfigOnFailure | Unset): Failure or budget fallback. REVIEW remains
                unpromoted; mint explicitly permits a new identity. Default: GraphResolverAdjudicationConfigOnFailure.REVIEW.
            description_inputs (GraphResolverAdjudicationConfigDescriptionInputs | Unset): Optional asset bindings.
                Resolution reads available data without waiting for promotion or producers.
            budget_ms (int | Unset): Total model-time budget per source artifact. Each call also obeys timeout_ms. Default:
                10000.
            max_pairs (int | Unset):  Default: 32.
            max_input_bytes (int | Unset):  Default: 16384.
            timeout_ms (int | Unset):  Default: 10000.
    """

    decider: DeciderConfig
    template: str
    question: ChoiceDecisionQuestion
    accept: GraphResolverAdjudicationAcceptance
    reject: GraphResolverAdjudicationAcceptance | Unset = UNSET
    on_failure: GraphResolverAdjudicationConfigOnFailure | Unset = GraphResolverAdjudicationConfigOnFailure.REVIEW
    description_inputs: GraphResolverAdjudicationConfigDescriptionInputs | Unset = UNSET
    budget_ms: int | Unset = 10000
    max_pairs: int | Unset = 32
    max_input_bytes: int | Unset = 16384
    timeout_ms: int | Unset = 10000

    def to_dict(self) -> dict[str, Any]:
        decider = self.decider.to_dict()

        template = self.template

        question = self.question.to_dict()

        accept = self.accept.to_dict()

        reject: dict[str, Any] | Unset = UNSET
        if not isinstance(self.reject, Unset):
            reject = self.reject.to_dict()

        on_failure: str | Unset = UNSET
        if not isinstance(self.on_failure, Unset):
            on_failure = self.on_failure.value

        description_inputs: dict[str, Any] | Unset = UNSET
        if not isinstance(self.description_inputs, Unset):
            description_inputs = self.description_inputs.to_dict()

        budget_ms = self.budget_ms

        max_pairs = self.max_pairs

        max_input_bytes = self.max_input_bytes

        timeout_ms = self.timeout_ms

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "decider": decider,
                "template": template,
                "question": question,
                "accept": accept,
            }
        )
        if reject is not UNSET:
            field_dict["reject"] = reject
        if on_failure is not UNSET:
            field_dict["on_failure"] = on_failure
        if description_inputs is not UNSET:
            field_dict["description_inputs"] = description_inputs
        if budget_ms is not UNSET:
            field_dict["budget_ms"] = budget_ms
        if max_pairs is not UNSET:
            field_dict["max_pairs"] = max_pairs
        if max_input_bytes is not UNSET:
            field_dict["max_input_bytes"] = max_input_bytes
        if timeout_ms is not UNSET:
            field_dict["timeout_ms"] = timeout_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.choice_decision_question import ChoiceDecisionQuestion
        from ..models.decider_config import DeciderConfig
        from ..models.graph_resolver_adjudication_acceptance import GraphResolverAdjudicationAcceptance
        from ..models.graph_resolver_adjudication_config_description_inputs import (
            GraphResolverAdjudicationConfigDescriptionInputs,
        )

        d = dict(src_dict)
        decider = DeciderConfig.from_dict(d.pop("decider"))

        template = d.pop("template")

        question = ChoiceDecisionQuestion.from_dict(d.pop("question"))

        accept = GraphResolverAdjudicationAcceptance.from_dict(d.pop("accept"))

        _reject = d.pop("reject", UNSET)
        reject: GraphResolverAdjudicationAcceptance | Unset
        if isinstance(_reject, Unset):
            reject = UNSET
        else:
            reject = GraphResolverAdjudicationAcceptance.from_dict(_reject)

        _on_failure = d.pop("on_failure", UNSET)
        on_failure: GraphResolverAdjudicationConfigOnFailure | Unset
        if isinstance(_on_failure, Unset):
            on_failure = UNSET
        else:
            on_failure = GraphResolverAdjudicationConfigOnFailure(_on_failure)

        _description_inputs = d.pop("description_inputs", UNSET)
        description_inputs: GraphResolverAdjudicationConfigDescriptionInputs | Unset
        if isinstance(_description_inputs, Unset):
            description_inputs = UNSET
        else:
            description_inputs = GraphResolverAdjudicationConfigDescriptionInputs.from_dict(_description_inputs)

        budget_ms = d.pop("budget_ms", UNSET)

        max_pairs = d.pop("max_pairs", UNSET)

        max_input_bytes = d.pop("max_input_bytes", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        graph_resolver_adjudication_config = cls(
            decider=decider,
            template=template,
            question=question,
            accept=accept,
            reject=reject,
            on_failure=on_failure,
            description_inputs=description_inputs,
            budget_ms=budget_ms,
            max_pairs=max_pairs,
            max_input_bytes=max_input_bytes,
            timeout_ms=timeout_ms,
        )

        return graph_resolver_adjudication_config
