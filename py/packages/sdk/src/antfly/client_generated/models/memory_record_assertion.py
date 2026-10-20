from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_record_assertion_kind import MemoryRecordAssertionKind
from ..models.memory_record_assertion_modality import MemoryRecordAssertionModality
from ..models.memory_record_assertion_role import MemoryRecordAssertionRole
from ..models.memory_record_assertion_stability import MemoryRecordAssertionStability
from ..models.memory_record_assertion_visibility import MemoryRecordAssertionVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_assertion_embedding_model_type_0 import MemoryRecordAssertionEmbeddingModelType0
    from ..models.memory_record_assertion_evidence_item import MemoryRecordAssertionEvidenceItem
    from ..models.memory_record_assertion_support_sets_item import MemoryRecordAssertionSupportSetsItem


T = TypeVar("T", bound="MemoryRecordAssertion")


@_attrs_define
class MemoryRecordAssertion:
    """
    Attributes:
        id (str):
        subject (str):
        predicate (str):
        value (str):
        statement (str):
        evidence (list[MemoryRecordAssertionEvidenceItem]):
        kind (MemoryRecordAssertionKind | Unset):
        scope (str | Unset):
        task (None | str | Unset):
        environment (None | str | Unset):
        role (MemoryRecordAssertionRole | Unset):
        modality (MemoryRecordAssertionModality | Unset):
        stability (MemoryRecordAssertionStability | Unset):
        valid_from (int | None | Unset):
        valid_to (int | None | Unset):
        depends_on (list[str] | Unset):
        support_sets (list[MemoryRecordAssertionSupportSetsItem] | Unset):
        supersedes (list[str] | Unset):
        motivates (list[str] | Unset):
        rejected_in_favor_of (list[str] | Unset):
        preconditions (list[str] | Unset):
        steps (list[str] | Unset):
        expected_outcome (None | str | Unset):
        producer (str | Unset):
        visibility (MemoryRecordAssertionVisibility | Unset):
        embedding (list[float] | None | Unset):
        embedding_model (MemoryRecordAssertionEmbeddingModelType0 | None | Unset):
        reconciliation_reason (None | str | Unset):
    """

    id: str
    subject: str
    predicate: str
    value: str
    statement: str
    evidence: list[MemoryRecordAssertionEvidenceItem]
    kind: MemoryRecordAssertionKind | Unset = UNSET
    scope: str | Unset = UNSET
    task: None | str | Unset = UNSET
    environment: None | str | Unset = UNSET
    role: MemoryRecordAssertionRole | Unset = UNSET
    modality: MemoryRecordAssertionModality | Unset = UNSET
    stability: MemoryRecordAssertionStability | Unset = UNSET
    valid_from: int | None | Unset = UNSET
    valid_to: int | None | Unset = UNSET
    depends_on: list[str] | Unset = UNSET
    support_sets: list[MemoryRecordAssertionSupportSetsItem] | Unset = UNSET
    supersedes: list[str] | Unset = UNSET
    motivates: list[str] | Unset = UNSET
    rejected_in_favor_of: list[str] | Unset = UNSET
    preconditions: list[str] | Unset = UNSET
    steps: list[str] | Unset = UNSET
    expected_outcome: None | str | Unset = UNSET
    producer: str | Unset = UNSET
    visibility: MemoryRecordAssertionVisibility | Unset = UNSET
    embedding: list[float] | None | Unset = UNSET
    embedding_model: MemoryRecordAssertionEmbeddingModelType0 | None | Unset = UNSET
    reconciliation_reason: None | str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_record_assertion_embedding_model_type_0 import MemoryRecordAssertionEmbeddingModelType0

        id = self.id

        subject = self.subject

        predicate = self.predicate

        value = self.value

        statement = self.statement

        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        scope = self.scope

        task: None | str | Unset
        if isinstance(self.task, Unset):
            task = UNSET
        else:
            task = self.task

        environment: None | str | Unset
        if isinstance(self.environment, Unset):
            environment = UNSET
        else:
            environment = self.environment

        role: str | Unset = UNSET
        if not isinstance(self.role, Unset):
            role = self.role.value

        modality: str | Unset = UNSET
        if not isinstance(self.modality, Unset):
            modality = self.modality.value

        stability: str | Unset = UNSET
        if not isinstance(self.stability, Unset):
            stability = self.stability.value

        valid_from: int | None | Unset
        if isinstance(self.valid_from, Unset):
            valid_from = UNSET
        else:
            valid_from = self.valid_from

        valid_to: int | None | Unset
        if isinstance(self.valid_to, Unset):
            valid_to = UNSET
        else:
            valid_to = self.valid_to

        depends_on: list[str] | Unset = UNSET
        if not isinstance(self.depends_on, Unset):
            depends_on = self.depends_on

        support_sets: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.support_sets, Unset):
            support_sets = []
            for support_sets_item_data in self.support_sets:
                support_sets_item = support_sets_item_data.to_dict()
                support_sets.append(support_sets_item)

        supersedes: list[str] | Unset = UNSET
        if not isinstance(self.supersedes, Unset):
            supersedes = self.supersedes

        motivates: list[str] | Unset = UNSET
        if not isinstance(self.motivates, Unset):
            motivates = self.motivates

        rejected_in_favor_of: list[str] | Unset = UNSET
        if not isinstance(self.rejected_in_favor_of, Unset):
            rejected_in_favor_of = self.rejected_in_favor_of

        preconditions: list[str] | Unset = UNSET
        if not isinstance(self.preconditions, Unset):
            preconditions = self.preconditions

        steps: list[str] | Unset = UNSET
        if not isinstance(self.steps, Unset):
            steps = self.steps

        expected_outcome: None | str | Unset
        if isinstance(self.expected_outcome, Unset):
            expected_outcome = UNSET
        else:
            expected_outcome = self.expected_outcome

        producer = self.producer

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        embedding: list[float] | None | Unset
        if isinstance(self.embedding, Unset):
            embedding = UNSET
        elif isinstance(self.embedding, list):
            embedding = self.embedding

        else:
            embedding = self.embedding

        embedding_model: dict[str, Any] | None | Unset
        if isinstance(self.embedding_model, Unset):
            embedding_model = UNSET
        elif isinstance(self.embedding_model, MemoryRecordAssertionEmbeddingModelType0):
            embedding_model = self.embedding_model.to_dict()
        else:
            embedding_model = self.embedding_model

        reconciliation_reason: None | str | Unset
        if isinstance(self.reconciliation_reason, Unset):
            reconciliation_reason = UNSET
        else:
            reconciliation_reason = self.reconciliation_reason

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "subject": subject,
                "predicate": predicate,
                "value": value,
                "statement": statement,
                "evidence": evidence,
            }
        )
        if kind is not UNSET:
            field_dict["kind"] = kind
        if scope is not UNSET:
            field_dict["scope"] = scope
        if task is not UNSET:
            field_dict["task"] = task
        if environment is not UNSET:
            field_dict["environment"] = environment
        if role is not UNSET:
            field_dict["role"] = role
        if modality is not UNSET:
            field_dict["modality"] = modality
        if stability is not UNSET:
            field_dict["stability"] = stability
        if valid_from is not UNSET:
            field_dict["valid_from"] = valid_from
        if valid_to is not UNSET:
            field_dict["valid_to"] = valid_to
        if depends_on is not UNSET:
            field_dict["depends_on"] = depends_on
        if support_sets is not UNSET:
            field_dict["support_sets"] = support_sets
        if supersedes is not UNSET:
            field_dict["supersedes"] = supersedes
        if motivates is not UNSET:
            field_dict["motivates"] = motivates
        if rejected_in_favor_of is not UNSET:
            field_dict["rejected_in_favor_of"] = rejected_in_favor_of
        if preconditions is not UNSET:
            field_dict["preconditions"] = preconditions
        if steps is not UNSET:
            field_dict["steps"] = steps
        if expected_outcome is not UNSET:
            field_dict["expected_outcome"] = expected_outcome
        if producer is not UNSET:
            field_dict["producer"] = producer
        if visibility is not UNSET:
            field_dict["visibility"] = visibility
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if embedding_model is not UNSET:
            field_dict["embedding_model"] = embedding_model
        if reconciliation_reason is not UNSET:
            field_dict["reconciliation_reason"] = reconciliation_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_assertion_embedding_model_type_0 import MemoryRecordAssertionEmbeddingModelType0
        from ..models.memory_record_assertion_evidence_item import MemoryRecordAssertionEvidenceItem
        from ..models.memory_record_assertion_support_sets_item import MemoryRecordAssertionSupportSetsItem

        d = dict(src_dict)
        id = d.pop("id")

        subject = d.pop("subject")

        predicate = d.pop("predicate")

        value = d.pop("value")

        statement = d.pop("statement")

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryRecordAssertionEvidenceItem.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        _kind = d.pop("kind", UNSET)
        kind: MemoryRecordAssertionKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = MemoryRecordAssertionKind(_kind)

        scope = d.pop("scope", UNSET)

        def _parse_task(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        task = _parse_task(d.pop("task", UNSET))

        def _parse_environment(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        environment = _parse_environment(d.pop("environment", UNSET))

        _role = d.pop("role", UNSET)
        role: MemoryRecordAssertionRole | Unset
        if isinstance(_role, Unset):
            role = UNSET
        else:
            role = MemoryRecordAssertionRole(_role)

        _modality = d.pop("modality", UNSET)
        modality: MemoryRecordAssertionModality | Unset
        if isinstance(_modality, Unset):
            modality = UNSET
        else:
            modality = MemoryRecordAssertionModality(_modality)

        _stability = d.pop("stability", UNSET)
        stability: MemoryRecordAssertionStability | Unset
        if isinstance(_stability, Unset):
            stability = UNSET
        else:
            stability = MemoryRecordAssertionStability(_stability)

        def _parse_valid_from(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        valid_from = _parse_valid_from(d.pop("valid_from", UNSET))

        def _parse_valid_to(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        valid_to = _parse_valid_to(d.pop("valid_to", UNSET))

        depends_on = cast(list[str], d.pop("depends_on", UNSET))

        _support_sets = d.pop("support_sets", UNSET)
        support_sets: list[MemoryRecordAssertionSupportSetsItem] | Unset = UNSET
        if _support_sets is not UNSET:
            support_sets = []
            for support_sets_item_data in _support_sets:
                support_sets_item = MemoryRecordAssertionSupportSetsItem.from_dict(support_sets_item_data)

                support_sets.append(support_sets_item)

        supersedes = cast(list[str], d.pop("supersedes", UNSET))

        motivates = cast(list[str], d.pop("motivates", UNSET))

        rejected_in_favor_of = cast(list[str], d.pop("rejected_in_favor_of", UNSET))

        preconditions = cast(list[str], d.pop("preconditions", UNSET))

        steps = cast(list[str], d.pop("steps", UNSET))

        def _parse_expected_outcome(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        expected_outcome = _parse_expected_outcome(d.pop("expected_outcome", UNSET))

        producer = d.pop("producer", UNSET)

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryRecordAssertionVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryRecordAssertionVisibility(_visibility)

        def _parse_embedding(data: object) -> list[float] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                embedding_type_0 = cast(list[float], data)

                return embedding_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[float] | None | Unset, data)

        embedding = _parse_embedding(d.pop("embedding", UNSET))

        def _parse_embedding_model(data: object) -> MemoryRecordAssertionEmbeddingModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                embedding_model_type_0 = MemoryRecordAssertionEmbeddingModelType0.from_dict(data)

                return embedding_model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecordAssertionEmbeddingModelType0 | None | Unset, data)

        embedding_model = _parse_embedding_model(d.pop("embedding_model", UNSET))

        def _parse_reconciliation_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        reconciliation_reason = _parse_reconciliation_reason(d.pop("reconciliation_reason", UNSET))

        memory_record_assertion = cls(
            id=id,
            subject=subject,
            predicate=predicate,
            value=value,
            statement=statement,
            evidence=evidence,
            kind=kind,
            scope=scope,
            task=task,
            environment=environment,
            role=role,
            modality=modality,
            stability=stability,
            valid_from=valid_from,
            valid_to=valid_to,
            depends_on=depends_on,
            support_sets=support_sets,
            supersedes=supersedes,
            motivates=motivates,
            rejected_in_favor_of=rejected_in_favor_of,
            preconditions=preconditions,
            steps=steps,
            expected_outcome=expected_outcome,
            producer=producer,
            visibility=visibility,
            embedding=embedding,
            embedding_model=embedding_model,
            reconciliation_reason=reconciliation_reason,
        )

        return memory_record_assertion
