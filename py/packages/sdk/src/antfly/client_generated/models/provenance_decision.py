from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.provenance_decision_status import ProvenanceDecisionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.provenance_decision_configuration import ProvenanceDecisionConfiguration
    from ..models.provenance_decision_executor import ProvenanceDecisionExecutor
    from ..models.provenance_decision_initiator import ProvenanceDecisionInitiator
    from ..models.provenance_decision_operation import ProvenanceDecisionOperation
    from ..models.provenance_decision_origin_type_0 import ProvenanceDecisionOriginType0
    from ..models.provenance_decision_resource import ProvenanceDecisionResource


T = TypeVar("T", bound="ProvenanceDecision")


@_attrs_define
class ProvenanceDecision:
    """
    Attributes:
        resource (ProvenanceDecisionResource):
        operation (ProvenanceDecisionOperation):
        action (str):
        status (ProvenanceDecisionStatus):
        initiator (ProvenanceDecisionInitiator):
        executor (ProvenanceDecisionExecutor):
        decided_at (int):
        applied_at (int | None):
        recorded_at (int):
        configuration (ProvenanceDecisionConfiguration):
        ontology_version (str):
        policy_version (str):
        schema_version (int | Unset):  Default: 1.
        origin (None | ProvenanceDecisionOriginType0 | Unset):
        effective_at (int | None | Unset):
    """

    resource: ProvenanceDecisionResource
    operation: ProvenanceDecisionOperation
    action: str
    status: ProvenanceDecisionStatus
    initiator: ProvenanceDecisionInitiator
    executor: ProvenanceDecisionExecutor
    decided_at: int
    applied_at: int | None
    recorded_at: int
    configuration: ProvenanceDecisionConfiguration
    ontology_version: str
    policy_version: str
    schema_version: int | Unset = 1
    origin: None | ProvenanceDecisionOriginType0 | Unset = UNSET
    effective_at: int | None | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.provenance_decision_origin_type_0 import ProvenanceDecisionOriginType0

        resource = self.resource.to_dict()

        operation = self.operation.to_dict()

        action = self.action

        status = self.status.value

        initiator = self.initiator.to_dict()

        executor = self.executor.to_dict()

        decided_at = self.decided_at

        applied_at: int | None
        applied_at = self.applied_at

        recorded_at = self.recorded_at

        configuration = self.configuration.to_dict()

        ontology_version = self.ontology_version

        policy_version = self.policy_version

        schema_version = self.schema_version

        origin: dict[str, Any] | None | Unset
        if isinstance(self.origin, Unset):
            origin = UNSET
        elif isinstance(self.origin, ProvenanceDecisionOriginType0):
            origin = self.origin.to_dict()
        else:
            origin = self.origin

        effective_at: int | None | Unset
        if isinstance(self.effective_at, Unset):
            effective_at = UNSET
        else:
            effective_at = self.effective_at

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "resource": resource,
                "operation": operation,
                "action": action,
                "status": status,
                "initiator": initiator,
                "executor": executor,
                "decided_at": decided_at,
                "applied_at": applied_at,
                "recorded_at": recorded_at,
                "configuration": configuration,
                "ontology_version": ontology_version,
                "policy_version": policy_version,
            }
        )
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if origin is not UNSET:
            field_dict["origin"] = origin
        if effective_at is not UNSET:
            field_dict["effective_at"] = effective_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.provenance_decision_configuration import ProvenanceDecisionConfiguration
        from ..models.provenance_decision_executor import ProvenanceDecisionExecutor
        from ..models.provenance_decision_initiator import ProvenanceDecisionInitiator
        from ..models.provenance_decision_operation import ProvenanceDecisionOperation
        from ..models.provenance_decision_origin_type_0 import ProvenanceDecisionOriginType0
        from ..models.provenance_decision_resource import ProvenanceDecisionResource

        d = dict(src_dict)
        resource = ProvenanceDecisionResource.from_dict(d.pop("resource"))

        operation = ProvenanceDecisionOperation.from_dict(d.pop("operation"))

        action = d.pop("action")

        status = ProvenanceDecisionStatus(d.pop("status"))

        initiator = ProvenanceDecisionInitiator.from_dict(d.pop("initiator"))

        executor = ProvenanceDecisionExecutor.from_dict(d.pop("executor"))

        decided_at = d.pop("decided_at")

        def _parse_applied_at(data: object) -> int | None:
            if data is None:
                return data
            return cast(int | None, data)

        applied_at = _parse_applied_at(d.pop("applied_at"))

        recorded_at = d.pop("recorded_at")

        configuration = ProvenanceDecisionConfiguration.from_dict(d.pop("configuration"))

        ontology_version = d.pop("ontology_version")

        policy_version = d.pop("policy_version")

        schema_version = d.pop("schema_version", UNSET)

        def _parse_origin(data: object) -> None | ProvenanceDecisionOriginType0 | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                origin_type_0 = ProvenanceDecisionOriginType0.from_dict(data)

                return origin_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | ProvenanceDecisionOriginType0 | Unset, data)

        origin = _parse_origin(d.pop("origin", UNSET))

        def _parse_effective_at(data: object) -> int | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(int | None | Unset, data)

        effective_at = _parse_effective_at(d.pop("effective_at", UNSET))

        provenance_decision = cls(
            resource=resource,
            operation=operation,
            action=action,
            status=status,
            initiator=initiator,
            executor=executor,
            decided_at=decided_at,
            applied_at=applied_at,
            recorded_at=recorded_at,
            configuration=configuration,
            ontology_version=ontology_version,
            policy_version=policy_version,
            schema_version=schema_version,
            origin=origin,
            effective_at=effective_at,
        )

        return provenance_decision
