from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.managed_source_destination_contract_accepted_by import ManagedSourceDestinationContractAcceptedBy
from ..models.managed_source_destination_contract_adapter import ManagedSourceDestinationContractAdapter
from ..models.managed_source_destination_contract_committed_by import ManagedSourceDestinationContractCommittedBy
from ..models.managed_source_destination_contract_committed_history import (
    ManagedSourceDestinationContractCommittedHistory,
)
from ..models.managed_source_destination_contract_input import ManagedSourceDestinationContractInput
from ..models.managed_source_destination_contract_transaction_scope import (
    ManagedSourceDestinationContractTransactionScope,
)
from ..models.managed_source_destination_contract_version import ManagedSourceDestinationContractVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.managed_source_destination_contract_outbound import ManagedSourceDestinationContractOutbound


T = TypeVar("T", bound="ManagedSourceDestinationContract")


@_attrs_define
class ManagedSourceDestinationContract:
    """
    Attributes:
        version (ManagedSourceDestinationContractVersion):
        input_ (ManagedSourceDestinationContractInput):
        accepted_by (ManagedSourceDestinationContractAcceptedBy):
        committed_by (ManagedSourceDestinationContractCommittedBy):
        transaction_scope (ManagedSourceDestinationContractTransactionScope):
        committed_history (ManagedSourceDestinationContractCommittedHistory):
        outbound (ManagedSourceDestinationContractOutbound):
        adapter (ManagedSourceDestinationContractAdapter | Unset):
    """

    version: ManagedSourceDestinationContractVersion
    input_: ManagedSourceDestinationContractInput
    accepted_by: ManagedSourceDestinationContractAcceptedBy
    committed_by: ManagedSourceDestinationContractCommittedBy
    transaction_scope: ManagedSourceDestinationContractTransactionScope
    committed_history: ManagedSourceDestinationContractCommittedHistory
    outbound: ManagedSourceDestinationContractOutbound
    adapter: ManagedSourceDestinationContractAdapter | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version.value

        input_ = self.input_.value

        accepted_by = self.accepted_by.value

        committed_by = self.committed_by.value

        transaction_scope = self.transaction_scope.value

        committed_history = self.committed_history.value

        outbound = self.outbound.to_dict()

        adapter: str | Unset = UNSET
        if not isinstance(self.adapter, Unset):
            adapter = self.adapter.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "version": version,
                "input": input_,
                "accepted_by": accepted_by,
                "committed_by": committed_by,
                "transaction_scope": transaction_scope,
                "committed_history": committed_history,
                "outbound": outbound,
            }
        )
        if adapter is not UNSET:
            field_dict["adapter"] = adapter

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.managed_source_destination_contract_outbound import ManagedSourceDestinationContractOutbound

        d = dict(src_dict)
        version = ManagedSourceDestinationContractVersion(d.pop("version"))

        input_ = ManagedSourceDestinationContractInput(d.pop("input"))

        accepted_by = ManagedSourceDestinationContractAcceptedBy(d.pop("accepted_by"))

        committed_by = ManagedSourceDestinationContractCommittedBy(d.pop("committed_by"))

        transaction_scope = ManagedSourceDestinationContractTransactionScope(d.pop("transaction_scope"))

        committed_history = ManagedSourceDestinationContractCommittedHistory(d.pop("committed_history"))

        outbound = ManagedSourceDestinationContractOutbound.from_dict(d.pop("outbound"))

        _adapter = d.pop("adapter", UNSET)
        adapter: ManagedSourceDestinationContractAdapter | Unset
        if isinstance(_adapter, Unset):
            adapter = UNSET
        else:
            adapter = ManagedSourceDestinationContractAdapter(_adapter)

        managed_source_destination_contract = cls(
            version=version,
            input_=input_,
            accepted_by=accepted_by,
            committed_by=committed_by,
            transaction_scope=transaction_scope,
            committed_history=committed_history,
            outbound=outbound,
            adapter=adapter,
        )

        managed_source_destination_contract.additional_properties = d
        return managed_source_destination_contract

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
