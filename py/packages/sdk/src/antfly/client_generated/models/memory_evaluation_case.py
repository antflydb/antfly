from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_evaluation_case_expected import MemoryEvaluationCaseExpected
from ..models.memory_evaluation_case_task import MemoryEvaluationCaseTask
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_evaluation_case_evidence_item import MemoryEvaluationCaseEvidenceItem
    from ..models.memory_evaluation_policy_input import MemoryEvaluationPolicyInput


T = TypeVar("T", bound="MemoryEvaluationCase")


@_attrs_define
class MemoryEvaluationCase:
    """
    Attributes:
        id (str):
        task (MemoryEvaluationCaseTask):
        input_ (Any):
        policy (MemoryEvaluationPolicyInput | None | Unset):
        expected (MemoryEvaluationCaseExpected | Unset):
        expected_supersedes (bool | None | Unset):
        relevant_ids (list[str] | Unset):
        evidence (list[MemoryEvaluationCaseEvidenceItem] | Unset):
    """

    id: str
    task: MemoryEvaluationCaseTask
    input_: Any
    policy: MemoryEvaluationPolicyInput | None | Unset = UNSET
    expected: MemoryEvaluationCaseExpected | Unset = UNSET
    expected_supersedes: bool | None | Unset = UNSET
    relevant_ids: list[str] | Unset = UNSET
    evidence: list[MemoryEvaluationCaseEvidenceItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_evaluation_policy_input import MemoryEvaluationPolicyInput

        id = self.id

        task = self.task.value

        input_ = self.input_

        policy: dict[str, Any] | None | Unset
        if isinstance(self.policy, Unset):
            policy = UNSET
        elif isinstance(self.policy, MemoryEvaluationPolicyInput):
            policy = self.policy.to_dict()
        else:
            policy = self.policy

        expected: str | Unset = UNSET
        if not isinstance(self.expected, Unset):
            expected = self.expected.value

        expected_supersedes: bool | None | Unset
        if isinstance(self.expected_supersedes, Unset):
            expected_supersedes = UNSET
        else:
            expected_supersedes = self.expected_supersedes

        relevant_ids: list[str] | Unset = UNSET
        if not isinstance(self.relevant_ids, Unset):
            relevant_ids = self.relevant_ids

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "task": task,
                "input": input_,
            }
        )
        if policy is not UNSET:
            field_dict["policy"] = policy
        if expected is not UNSET:
            field_dict["expected"] = expected
        if expected_supersedes is not UNSET:
            field_dict["expected_supersedes"] = expected_supersedes
        if relevant_ids is not UNSET:
            field_dict["relevant_ids"] = relevant_ids
        if evidence is not UNSET:
            field_dict["evidence"] = evidence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_evaluation_case_evidence_item import MemoryEvaluationCaseEvidenceItem
        from ..models.memory_evaluation_policy_input import MemoryEvaluationPolicyInput

        d = dict(src_dict)
        id = d.pop("id")

        task = MemoryEvaluationCaseTask(d.pop("task"))

        input_ = d.pop("input")

        def _parse_policy(data: object) -> MemoryEvaluationPolicyInput | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                policy_type_1 = MemoryEvaluationPolicyInput.from_dict(data)

                return policy_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryEvaluationPolicyInput | None | Unset, data)

        policy = _parse_policy(d.pop("policy", UNSET))

        _expected = d.pop("expected", UNSET)
        expected: MemoryEvaluationCaseExpected | Unset
        if isinstance(_expected, Unset):
            expected = UNSET
        else:
            expected = MemoryEvaluationCaseExpected(_expected)

        def _parse_expected_supersedes(data: object) -> bool | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(bool | None | Unset, data)

        expected_supersedes = _parse_expected_supersedes(d.pop("expected_supersedes", UNSET))

        relevant_ids = cast(list[str], d.pop("relevant_ids", UNSET))

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemoryEvaluationCaseEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemoryEvaluationCaseEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        memory_evaluation_case = cls(
            id=id,
            task=task,
            input_=input_,
            policy=policy,
            expected=expected,
            expected_supersedes=expected_supersedes,
            relevant_ids=relevant_ids,
            evidence=evidence,
        )

        return memory_evaluation_case
