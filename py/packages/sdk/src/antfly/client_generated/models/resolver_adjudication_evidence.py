from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.resolver_adjudication_evidence_status import ResolverAdjudicationEvidenceStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.resolver_adjudication_evidence_response_type_0 import ResolverAdjudicationEvidenceResponseType0


T = TypeVar("T", bound="ResolverAdjudicationEvidence")


@_attrs_define
class ResolverAdjudicationEvidence:
    """
    Attributes:
        execution_hash (str | Unset):
        specification_hash (str | Unset):
        input_hash (str | Unset):
        candidate_hash (str | Unset):
        candidate_table (str | Unset):
        candidate_key (str | Unset):
        deterministic_probability (float | Unset):
        status (ResolverAdjudicationEvidenceStatus | Unset):
        response (None | ResolverAdjudicationEvidenceResponseType0 | Unset):
        failure (None | str | Unset):
    """

    execution_hash: str | Unset = UNSET
    specification_hash: str | Unset = UNSET
    input_hash: str | Unset = UNSET
    candidate_hash: str | Unset = UNSET
    candidate_table: str | Unset = UNSET
    candidate_key: str | Unset = UNSET
    deterministic_probability: float | Unset = UNSET
    status: ResolverAdjudicationEvidenceStatus | Unset = UNSET
    response: None | ResolverAdjudicationEvidenceResponseType0 | Unset = UNSET
    failure: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.resolver_adjudication_evidence_response_type_0 import ResolverAdjudicationEvidenceResponseType0

        execution_hash = self.execution_hash

        specification_hash = self.specification_hash

        input_hash = self.input_hash

        candidate_hash = self.candidate_hash

        candidate_table = self.candidate_table

        candidate_key = self.candidate_key

        deterministic_probability = self.deterministic_probability

        status: str | Unset = UNSET
        if not isinstance(self.status, Unset):
            status = self.status.value

        response: dict[str, Any] | None | Unset
        if isinstance(self.response, Unset):
            response = UNSET
        elif isinstance(self.response, ResolverAdjudicationEvidenceResponseType0):
            response = self.response.to_dict()
        else:
            response = self.response

        failure: None | str | Unset
        if isinstance(self.failure, Unset):
            failure = UNSET
        else:
            failure = self.failure

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if execution_hash is not UNSET:
            field_dict["execution_hash"] = execution_hash
        if specification_hash is not UNSET:
            field_dict["specification_hash"] = specification_hash
        if input_hash is not UNSET:
            field_dict["input_hash"] = input_hash
        if candidate_hash is not UNSET:
            field_dict["candidate_hash"] = candidate_hash
        if candidate_table is not UNSET:
            field_dict["candidate_table"] = candidate_table
        if candidate_key is not UNSET:
            field_dict["candidate_key"] = candidate_key
        if deterministic_probability is not UNSET:
            field_dict["deterministic_probability"] = deterministic_probability
        if status is not UNSET:
            field_dict["status"] = status
        if response is not UNSET:
            field_dict["response"] = response
        if failure is not UNSET:
            field_dict["failure"] = failure

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.resolver_adjudication_evidence_response_type_0 import ResolverAdjudicationEvidenceResponseType0

        d = dict(src_dict)
        execution_hash = d.pop("execution_hash", UNSET)

        specification_hash = d.pop("specification_hash", UNSET)

        input_hash = d.pop("input_hash", UNSET)

        candidate_hash = d.pop("candidate_hash", UNSET)

        candidate_table = d.pop("candidate_table", UNSET)

        candidate_key = d.pop("candidate_key", UNSET)

        deterministic_probability = d.pop("deterministic_probability", UNSET)

        _status = d.pop("status", UNSET)
        status: ResolverAdjudicationEvidenceStatus | Unset
        if isinstance(_status, Unset):
            status = UNSET
        else:
            status = ResolverAdjudicationEvidenceStatus(_status)

        def _parse_response(data: object) -> None | ResolverAdjudicationEvidenceResponseType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_type_0 = ResolverAdjudicationEvidenceResponseType0.from_dict(data)

                return response_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ResolverAdjudicationEvidenceResponseType0 | Unset, data)

        response = _parse_response(d.pop("response", UNSET))

        def _parse_failure(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        failure = _parse_failure(d.pop("failure", UNSET))

        resolver_adjudication_evidence = cls(
            execution_hash=execution_hash,
            specification_hash=specification_hash,
            input_hash=input_hash,
            candidate_hash=candidate_hash,
            candidate_table=candidate_table,
            candidate_key=candidate_key,
            deterministic_probability=deterministic_probability,
            status=status,
            response=response,
            failure=failure,
        )

        resolver_adjudication_evidence.additional_properties = d
        return resolver_adjudication_evidence

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
