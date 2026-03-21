from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.evaluator_name import EvaluatorName
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.eval_options import EvalOptions
    from ..models.generator_config import GeneratorConfig
    from ..models.ground_truth import GroundTruth


T = TypeVar("T", bound="EvalConfig")


@_attrs_define
class EvalConfig:
    """Configuration for inline evaluation of query results.
    Add to RAGRequest, QueryRequest, or AnswerAgentRequest.

        Attributes:
            evaluators (Union[Unset, list[EvaluatorName]]): List of evaluators to run
            judge (Union[Unset, GeneratorConfig]): A unified configuration for a generative AI provider.
                 Example: {'provider': 'openai', 'model': 'gpt-4.1', 'temperature': 0.7, 'max_tokens': 2048}.
            ground_truth (Union[Unset, GroundTruth]): Ground truth data for evaluation
            options (Union[Unset, EvalOptions]): Options for evaluation behavior
    """

    evaluators: Union[Unset, list[EvaluatorName]] = UNSET
    judge: Union[Unset, "GeneratorConfig"] = UNSET
    ground_truth: Union[Unset, "GroundTruth"] = UNSET
    options: Union[Unset, "EvalOptions"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        evaluators: Union[Unset, list[str]] = UNSET
        if not isinstance(self.evaluators, Unset):
            evaluators = []
            for evaluators_item_data in self.evaluators:
                evaluators_item = evaluators_item_data.value
                evaluators.append(evaluators_item)

        judge: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.judge, Unset):
            judge = self.judge.to_dict()

        ground_truth: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.ground_truth, Unset):
            ground_truth = self.ground_truth.to_dict()

        options: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.options, Unset):
            options = self.options.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if evaluators is not UNSET:
            field_dict["evaluators"] = evaluators
        if judge is not UNSET:
            field_dict["judge"] = judge
        if ground_truth is not UNSET:
            field_dict["ground_truth"] = ground_truth
        if options is not UNSET:
            field_dict["options"] = options

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.eval_options import EvalOptions
        from ..models.generator_config import GeneratorConfig
        from ..models.ground_truth import GroundTruth

        d = dict(src_dict)
        evaluators = []
        _evaluators = d.pop("evaluators", UNSET)
        for evaluators_item_data in _evaluators or []:
            evaluators_item = EvaluatorName(evaluators_item_data)

            evaluators.append(evaluators_item)

        _judge = d.pop("judge", UNSET)
        judge: Union[Unset, GeneratorConfig]
        if isinstance(_judge, Unset):
            judge = UNSET
        else:
            judge = GeneratorConfig.from_dict(_judge)

        _ground_truth = d.pop("ground_truth", UNSET)
        ground_truth: Union[Unset, GroundTruth]
        if isinstance(_ground_truth, Unset):
            ground_truth = UNSET
        else:
            ground_truth = GroundTruth.from_dict(_ground_truth)

        _options = d.pop("options", UNSET)
        options: Union[Unset, EvalOptions]
        if isinstance(_options, Unset):
            options = UNSET
        else:
            options = EvalOptions.from_dict(_options)

        eval_config = cls(
            evaluators=evaluators,
            judge=judge,
            ground_truth=ground_truth,
            options=options,
        )

        eval_config.additional_properties = d
        return eval_config

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
