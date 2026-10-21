from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.memory_recall_request_kind import MemoryRecallRequestKind
from ..models.memory_recall_request_missing_relationship import MemoryRecallRequestMissingRelationship
from ..models.memory_recall_request_relationship import MemoryRecallRequestRelationship
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_recall_request_embedding_model_type_0 import MemoryRecallRequestEmbeddingModelType0


T = TypeVar("T", bound="MemoryRecallRequest")


@_attrs_define
class MemoryRecallRequest:
    """
    Attributes:
        query (str | Unset):
        relationship (MemoryRecallRequestRelationship | Unset):
        target_id (None | str | Unset):
        missing_relationship (MemoryRecallRequestMissingRelationship | Unset):
        subject (None | str | Unset):
        predicate (None | str | Unset):
        scope (None | str | Unset):
        environment (None | str | Unset):
        task (None | str | Unset):
        kind (MemoryRecallRequestKind | Unset):
        valid_at (int | None | Unset):
        known_at (int | None | Unset):
        include_history (bool | Unset):  Default: False.
        include_provisional (bool | Unset):  Default: True.
        limit (int | Unset):  Default: 20.
        token_budget (int | Unset):  Default: 2048.
        cursor (None | str | Unset):
        embedding (list[float] | None | Unset):
        embedding_model (MemoryRecallRequestEmbeddingModelType0 | None | Unset):
    """

    query: str | Unset = UNSET
    relationship: MemoryRecallRequestRelationship | Unset = UNSET
    target_id: None | str | Unset = UNSET
    missing_relationship: MemoryRecallRequestMissingRelationship | Unset = UNSET
    subject: None | str | Unset = UNSET
    predicate: None | str | Unset = UNSET
    scope: None | str | Unset = UNSET
    environment: None | str | Unset = UNSET
    task: None | str | Unset = UNSET
    kind: MemoryRecallRequestKind | Unset = UNSET
    valid_at: int | None | Unset = UNSET
    known_at: int | None | Unset = UNSET
    include_history: bool | Unset = False
    include_provisional: bool | Unset = True
    limit: int | Unset = 20
    token_budget: int | Unset = 2048
    cursor: None | str | Unset = UNSET
    embedding: list[float] | None | Unset = UNSET
    embedding_model: MemoryRecallRequestEmbeddingModelType0 | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.memory_recall_request_embedding_model_type_0 import MemoryRecallRequestEmbeddingModelType0

        query = self.query

        relationship: str | Unset = UNSET
        if not isinstance(self.relationship, Unset):
            relationship = self.relationship.value

        target_id: None | str | Unset
        if isinstance(self.target_id, Unset):
            target_id = UNSET
        else:
            target_id = self.target_id

        missing_relationship: str | Unset = UNSET
        if not isinstance(self.missing_relationship, Unset):
            missing_relationship = self.missing_relationship.value

        subject: None | str | Unset
        if isinstance(self.subject, Unset):
            subject = UNSET
        else:
            subject = self.subject

        predicate: None | str | Unset
        if isinstance(self.predicate, Unset):
            predicate = UNSET
        else:
            predicate = self.predicate

        scope: None | str | Unset
        if isinstance(self.scope, Unset):
            scope = UNSET
        else:
            scope = self.scope

        environment: None | str | Unset
        if isinstance(self.environment, Unset):
            environment = UNSET
        else:
            environment = self.environment

        task: None | str | Unset
        if isinstance(self.task, Unset):
            task = UNSET
        else:
            task = self.task

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        valid_at: int | None | Unset
        if isinstance(self.valid_at, Unset):
            valid_at = UNSET
        else:
            valid_at = self.valid_at

        known_at: int | None | Unset
        if isinstance(self.known_at, Unset):
            known_at = UNSET
        else:
            known_at = self.known_at

        include_history = self.include_history

        include_provisional = self.include_provisional

        limit = self.limit

        token_budget = self.token_budget

        cursor: None | str | Unset
        if isinstance(self.cursor, Unset):
            cursor = UNSET
        else:
            cursor = self.cursor

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
        elif isinstance(self.embedding_model, MemoryRecallRequestEmbeddingModelType0):
            embedding_model = self.embedding_model.to_dict()
        else:
            embedding_model = self.embedding_model

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if query is not UNSET:
            field_dict["query"] = query
        if relationship is not UNSET:
            field_dict["relationship"] = relationship
        if target_id is not UNSET:
            field_dict["target_id"] = target_id
        if missing_relationship is not UNSET:
            field_dict["missing_relationship"] = missing_relationship
        if subject is not UNSET:
            field_dict["subject"] = subject
        if predicate is not UNSET:
            field_dict["predicate"] = predicate
        if scope is not UNSET:
            field_dict["scope"] = scope
        if environment is not UNSET:
            field_dict["environment"] = environment
        if task is not UNSET:
            field_dict["task"] = task
        if kind is not UNSET:
            field_dict["kind"] = kind
        if valid_at is not UNSET:
            field_dict["valid_at"] = valid_at
        if known_at is not UNSET:
            field_dict["known_at"] = known_at
        if include_history is not UNSET:
            field_dict["include_history"] = include_history
        if include_provisional is not UNSET:
            field_dict["include_provisional"] = include_provisional
        if limit is not UNSET:
            field_dict["limit"] = limit
        if token_budget is not UNSET:
            field_dict["token_budget"] = token_budget
        if cursor is not UNSET:
            field_dict["cursor"] = cursor
        if embedding is not UNSET:
            field_dict["embedding"] = embedding
        if embedding_model is not UNSET:
            field_dict["embedding_model"] = embedding_model

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_recall_request_embedding_model_type_0 import MemoryRecallRequestEmbeddingModelType0

        d = dict(src_dict)
        query = d.pop("query", UNSET)

        _relationship = d.pop("relationship", UNSET)
        relationship: MemoryRecallRequestRelationship | Unset
        if isinstance(_relationship, Unset):
            relationship = UNSET
        else:
            relationship = MemoryRecallRequestRelationship(_relationship)

        def _parse_target_id(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        target_id = _parse_target_id(d.pop("target_id", UNSET))

        _missing_relationship = d.pop("missing_relationship", UNSET)
        missing_relationship: MemoryRecallRequestMissingRelationship | Unset
        if isinstance(_missing_relationship, Unset):
            missing_relationship = UNSET
        else:
            missing_relationship = MemoryRecallRequestMissingRelationship(_missing_relationship)

        def _parse_subject(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        subject = _parse_subject(d.pop("subject", UNSET))

        def _parse_predicate(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        predicate = _parse_predicate(d.pop("predicate", UNSET))

        def _parse_scope(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        scope = _parse_scope(d.pop("scope", UNSET))

        def _parse_environment(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        environment = _parse_environment(d.pop("environment", UNSET))

        def _parse_task(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        task = _parse_task(d.pop("task", UNSET))

        _kind = d.pop("kind", UNSET)
        kind: MemoryRecallRequestKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = MemoryRecallRequestKind(_kind)

        def _parse_valid_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        valid_at = _parse_valid_at(d.pop("valid_at", UNSET))

        def _parse_known_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        known_at = _parse_known_at(d.pop("known_at", UNSET))

        include_history = d.pop("include_history", UNSET)

        include_provisional = d.pop("include_provisional", UNSET)

        limit = d.pop("limit", UNSET)

        token_budget = d.pop("token_budget", UNSET)

        def _parse_cursor(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        cursor = _parse_cursor(d.pop("cursor", UNSET))

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

        def _parse_embedding_model(data: object) -> MemoryRecallRequestEmbeddingModelType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                embedding_model_type_0 = MemoryRecallRequestEmbeddingModelType0.from_dict(data)

                return embedding_model_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(MemoryRecallRequestEmbeddingModelType0 | None | Unset, data)

        embedding_model = _parse_embedding_model(d.pop("embedding_model", UNSET))

        memory_recall_request = cls(
            query=query,
            relationship=relationship,
            target_id=target_id,
            missing_relationship=missing_relationship,
            subject=subject,
            predicate=predicate,
            scope=scope,
            environment=environment,
            task=task,
            kind=kind,
            valid_at=valid_at,
            known_at=known_at,
            include_history=include_history,
            include_provisional=include_provisional,
            limit=limit,
            token_budget=token_budget,
            cursor=cursor,
            embedding=embedding,
            embedding_model=embedding_model,
        )

        return memory_recall_request
