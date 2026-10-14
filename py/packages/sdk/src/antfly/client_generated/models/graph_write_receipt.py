from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_write_receipt_status import GraphWriteReceiptStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_write_receipt_outcome_type_0 import GraphWriteReceiptOutcomeType0


T = TypeVar("T", bound="GraphWriteReceipt")


@_attrs_define
class GraphWriteReceipt:
    """
    Attributes:
        transaction_id (str): Native durable transaction identity; never replay a statement after an ambiguous write
            response.
        status (GraphWriteReceiptStatus):
        outcome (GraphWriteReceiptOutcomeType0 | None | Unset):
    """

    transaction_id: str
    status: GraphWriteReceiptStatus
    outcome: GraphWriteReceiptOutcomeType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_write_receipt_outcome_type_0 import GraphWriteReceiptOutcomeType0

        transaction_id = self.transaction_id

        status = self.status.value

        outcome: dict[str, Any] | None | Unset
        if isinstance(self.outcome, Unset):
            outcome = UNSET
        elif isinstance(self.outcome, GraphWriteReceiptOutcomeType0):
            outcome = self.outcome.to_dict()
        else:
            outcome = self.outcome

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "transaction_id": transaction_id,
                "status": status,
            }
        )
        if outcome is not UNSET:
            field_dict["outcome"] = outcome

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_write_receipt_outcome_type_0 import GraphWriteReceiptOutcomeType0

        d = dict(src_dict)
        transaction_id = d.pop("transaction_id")

        status = GraphWriteReceiptStatus(d.pop("status"))

        def _parse_outcome(data: object) -> GraphWriteReceiptOutcomeType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                outcome_type_0 = GraphWriteReceiptOutcomeType0.from_dict(data)

                return outcome_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(GraphWriteReceiptOutcomeType0 | None | Unset, data)

        outcome = _parse_outcome(d.pop("outcome", UNSET))

        graph_write_receipt = cls(
            transaction_id=transaction_id,
            status=status,
            outcome=outcome,
        )

        graph_write_receipt.additional_properties = d
        return graph_write_receipt

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
