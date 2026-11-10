from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryBriefRequest")


@_attrs_define
class MemoryBriefRequest:
    """
    Attributes:
        scope (None | str | Unset):
        task (None | str | Unset):
        environment (None | str | Unset):
        include_provisional (bool | Unset):  Default: False.
        limit (int | Unset):  Default: 16.
        token_budget (int | Unset):  Default: 1024.
    """

    scope: None | str | Unset = UNSET
    task: None | str | Unset = UNSET
    environment: None | str | Unset = UNSET
    include_provisional: bool | Unset = False
    limit: int | Unset = 16
    token_budget: int | Unset = 1024

    def to_dict(self) -> dict[str, Any]:
        scope: None | str | Unset
        if isinstance(self.scope, Unset):
            scope = UNSET
        else:
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

        include_provisional = self.include_provisional

        limit = self.limit

        token_budget = self.token_budget

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if scope is not UNSET:
            field_dict["scope"] = scope
        if task is not UNSET:
            field_dict["task"] = task
        if environment is not UNSET:
            field_dict["environment"] = environment
        if include_provisional is not UNSET:
            field_dict["include_provisional"] = include_provisional
        if limit is not UNSET:
            field_dict["limit"] = limit
        if token_budget is not UNSET:
            field_dict["token_budget"] = token_budget

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)

        def _parse_scope(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        scope = _parse_scope(d.pop("scope", UNSET))

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

        include_provisional = d.pop("include_provisional", UNSET)

        limit = d.pop("limit", UNSET)

        token_budget = d.pop("token_budget", UNSET)

        memory_brief_request = cls(
            scope=scope,
            task=task,
            environment=environment,
            include_provisional=include_provisional,
            limit=limit,
            token_budget=token_budget,
        )

        return memory_brief_request
