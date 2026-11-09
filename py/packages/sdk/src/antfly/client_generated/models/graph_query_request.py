from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_query_limits import GraphQueryLimits
    from ..models.graph_query_request_parameters import GraphQueryRequestParameters
    from ..models.graph_wire_value import GraphWireValue


T = TypeVar("T", bound="GraphQueryRequest")


@_attrs_define
class GraphQueryRequest:
    """
    Attributes:
        query (str):
        write_request_id (str | Unset): Client-generated native transaction ID for write recovery after cancellation or
            a lost response. Reusing an admitted ID is rejected; inspect its transaction status instead of replaying the
            statement.
        parameters (GraphQueryRequestParameters | Unset): Ordinary JSON parameters. Mutually exclusive with
            typed_parameters.
        typed_parameters (GraphWireValue | Unset): Version 1 graph value. INTEGER payloads are signed decimal strings.
            Map payloads contain typed values, keeping user keys distinct from type tags. Entity identities are opaque
            table-qualified handles.
        limits (GraphQueryLimits | Unset):
    """

    query: str
    write_request_id: str | Unset = UNSET
    parameters: GraphQueryRequestParameters | Unset = UNSET
    typed_parameters: GraphWireValue | Unset = UNSET
    limits: GraphQueryLimits | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        query = self.query

        write_request_id = self.write_request_id

        parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.parameters, Unset):
            parameters = self.parameters.to_dict()

        typed_parameters: dict[str, Any] | Unset = UNSET
        if not isinstance(self.typed_parameters, Unset):
            typed_parameters = self.typed_parameters.to_dict()

        limits: dict[str, Any] | Unset = UNSET
        if not isinstance(self.limits, Unset):
            limits = self.limits.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "query": query,
            }
        )
        if write_request_id is not UNSET:
            field_dict["write_request_id"] = write_request_id
        if parameters is not UNSET:
            field_dict["parameters"] = parameters
        if typed_parameters is not UNSET:
            field_dict["typed_parameters"] = typed_parameters
        if limits is not UNSET:
            field_dict["limits"] = limits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_query_limits import GraphQueryLimits
        from ..models.graph_query_request_parameters import GraphQueryRequestParameters
        from ..models.graph_wire_value import GraphWireValue

        d = dict(src_dict)
        query = d.pop("query")

        write_request_id = d.pop("write_request_id", UNSET)

        _parameters = d.pop("parameters", UNSET)
        parameters: GraphQueryRequestParameters | Unset
        if isinstance(_parameters, Unset):
            parameters = UNSET
        else:
            parameters = GraphQueryRequestParameters.from_dict(_parameters)

        _typed_parameters = d.pop("typed_parameters", UNSET)
        typed_parameters: GraphWireValue | Unset
        if isinstance(_typed_parameters, Unset):
            typed_parameters = UNSET
        else:
            typed_parameters = GraphWireValue.from_dict(_typed_parameters)

        _limits = d.pop("limits", UNSET)
        limits: GraphQueryLimits | Unset
        if isinstance(_limits, Unset):
            limits = UNSET
        else:
            limits = GraphQueryLimits.from_dict(_limits)

        graph_query_request = cls(
            query=query,
            write_request_id=write_request_id,
            parameters=parameters,
            typed_parameters=typed_parameters,
            limits=limits,
        )

        return graph_query_request
