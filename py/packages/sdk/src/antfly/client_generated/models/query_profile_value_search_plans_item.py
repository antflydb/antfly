from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.query_profile_value_search_plans_item_operation import QueryProfileValueSearchPlansItemOperation
from ..models.query_profile_value_search_plans_item_verification import QueryProfileValueSearchPlansItemVerification
from ..types import UNSET, Unset

T = TypeVar("T", bound="QueryProfileValueSearchPlansItem")


@_attrs_define
class QueryProfileValueSearchPlansItem:
    """
    Attributes:
        operation (QueryProfileValueSearchPlansItemOperation | Unset):
        field (str | Unset):
        candidate_field (None | str | Unset):
        index (None | str | Unset):
        verification (QueryProfileValueSearchPlansItemVerification | Unset): Membership proof; lossless_keyword uses
            literal postings without source loading or synonym expansion. Retained immutable cuts may reuse previously
            verified original-value membership.
        fallback_reason (None | str | Unset):
    """

    operation: QueryProfileValueSearchPlansItemOperation | Unset = UNSET
    field: str | Unset = UNSET
    candidate_field: None | str | Unset = UNSET
    index: None | str | Unset = UNSET
    verification: QueryProfileValueSearchPlansItemVerification | Unset = UNSET
    fallback_reason: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        operation: str | Unset = UNSET
        if not isinstance(self.operation, Unset):
            operation = self.operation.value

        field = self.field

        candidate_field: None | str | Unset
        if isinstance(self.candidate_field, Unset):
            candidate_field = UNSET
        else:
            candidate_field = self.candidate_field

        index: None | str | Unset
        if isinstance(self.index, Unset):
            index = UNSET
        else:
            index = self.index

        verification: str | Unset = UNSET
        if not isinstance(self.verification, Unset):
            verification = self.verification.value

        fallback_reason: None | str | Unset
        if isinstance(self.fallback_reason, Unset):
            fallback_reason = UNSET
        else:
            fallback_reason = self.fallback_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if operation is not UNSET:
            field_dict["operation"] = operation
        if field is not UNSET:
            field_dict["field"] = field
        if candidate_field is not UNSET:
            field_dict["candidate_field"] = candidate_field
        if index is not UNSET:
            field_dict["index"] = index
        if verification is not UNSET:
            field_dict["verification"] = verification
        if fallback_reason is not UNSET:
            field_dict["fallback_reason"] = fallback_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _operation = d.pop("operation", UNSET)
        operation: QueryProfileValueSearchPlansItemOperation | Unset
        if isinstance(_operation, Unset):
            operation = UNSET
        else:
            operation = QueryProfileValueSearchPlansItemOperation(_operation)

        field = d.pop("field", UNSET)

        def _parse_candidate_field(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        candidate_field = _parse_candidate_field(d.pop("candidate_field", UNSET))

        def _parse_index(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        index = _parse_index(d.pop("index", UNSET))

        _verification = d.pop("verification", UNSET)
        verification: QueryProfileValueSearchPlansItemVerification | Unset
        if isinstance(_verification, Unset):
            verification = UNSET
        else:
            verification = QueryProfileValueSearchPlansItemVerification(_verification)

        def _parse_fallback_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        fallback_reason = _parse_fallback_reason(d.pop("fallback_reason", UNSET))

        query_profile_value_search_plans_item = cls(
            operation=operation,
            field=field,
            candidate_field=candidate_field,
            index=index,
            verification=verification,
            fallback_reason=fallback_reason,
        )

        query_profile_value_search_plans_item.additional_properties = d
        return query_profile_value_search_plans_item

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
