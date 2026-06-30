from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.rows_aggregate_plan_request import RowsAggregatePlanRequest
from ...models.rows_aggregate_result_set import RowsAggregateResultSet
from ...models.rows_join_plan_request import RowsJoinPlanRequest
from ...models.rows_lateral_plan_request import RowsLateralPlanRequest
from ...models.rows_query_plan_request import RowsQueryPlanRequest
from ...models.rows_query_result_set import RowsQueryResultSet
from ...models.rows_stream_result_set import RowsStreamResultSet
from ...models.rows_window_plan_request import RowsWindowPlanRequest
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RowsAggregatePlanRequest
    | RowsJoinPlanRequest
    | RowsLateralPlanRequest
    | RowsQueryPlanRequest
    | RowsWindowPlanRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/plan".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if isinstance(body, RowsQueryPlanRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, RowsAggregatePlanRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, RowsWindowPlanRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, RowsJoinPlanRequest):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet | None:
    if response.status_code == 200:

        def _parse_response_200(data: object) -> RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_200_type_0 = RowsQueryResultSet.from_dict(data)

                return response_200_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_200_type_1 = RowsAggregateResultSet.from_dict(data)

                return response_200_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_200_type_2 = RowsStreamResultSet.from_dict(data)

            return response_200_type_2

        response_200 = _parse_response_200(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = Error.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 501:
        response_501 = Error.from_dict(response.json())

        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsAggregatePlanRequest
    | RowsJoinPlanRequest
    | RowsLateralPlanRequest
    | RowsQueryPlanRequest
    | RowsWindowPlanRequest,
) -> Response[Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet]:
    """Execute a typed relational row read plan

     Executes exactly one typed read-plan envelope. This endpoint accepts the
    same query, aggregate, window, join, and lateral plan bodies as the
    operation-specific endpoints and dispatches them by the single operation
    branch present in the request.

    Args:
        table_name (str):
        body (RowsAggregatePlanRequest | RowsJoinPlanRequest | RowsLateralPlanRequest |
            RowsQueryPlanRequest | RowsWindowPlanRequest): Generic typed relational row plan envelope.
            It is exactly one
            operation-specific envelope: query, aggregate, window, join, or
            lateral. Query, aggregate, and window plans use `ranges`; join and
            lateral plans use paired `left_ranges` and `right_ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsAggregatePlanRequest
    | RowsJoinPlanRequest
    | RowsLateralPlanRequest
    | RowsQueryPlanRequest
    | RowsWindowPlanRequest,
) -> Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet | None:
    """Execute a typed relational row read plan

     Executes exactly one typed read-plan envelope. This endpoint accepts the
    same query, aggregate, window, join, and lateral plan bodies as the
    operation-specific endpoints and dispatches them by the single operation
    branch present in the request.

    Args:
        table_name (str):
        body (RowsAggregatePlanRequest | RowsJoinPlanRequest | RowsLateralPlanRequest |
            RowsQueryPlanRequest | RowsWindowPlanRequest): Generic typed relational row plan envelope.
            It is exactly one
            operation-specific envelope: query, aggregate, window, join, or
            lateral. Query, aggregate, and window plans use `ranges`; join and
            lateral plans use paired `left_ranges` and `right_ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsAggregatePlanRequest
    | RowsJoinPlanRequest
    | RowsLateralPlanRequest
    | RowsQueryPlanRequest
    | RowsWindowPlanRequest,
) -> Response[Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet]:
    """Execute a typed relational row read plan

     Executes exactly one typed read-plan envelope. This endpoint accepts the
    same query, aggregate, window, join, and lateral plan bodies as the
    operation-specific endpoints and dispatches them by the single operation
    branch present in the request.

    Args:
        table_name (str):
        body (RowsAggregatePlanRequest | RowsJoinPlanRequest | RowsLateralPlanRequest |
            RowsQueryPlanRequest | RowsWindowPlanRequest): Generic typed relational row plan envelope.
            It is exactly one
            operation-specific envelope: query, aggregate, window, join, or
            lateral. Query, aggregate, and window plans use `ranges`; join and
            lateral plans use paired `left_ranges` and `right_ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RowsAggregatePlanRequest
    | RowsJoinPlanRequest
    | RowsLateralPlanRequest
    | RowsQueryPlanRequest
    | RowsWindowPlanRequest,
) -> Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet | None:
    """Execute a typed relational row read plan

     Executes exactly one typed read-plan envelope. This endpoint accepts the
    same query, aggregate, window, join, and lateral plan bodies as the
    operation-specific endpoints and dispatches them by the single operation
    branch present in the request.

    Args:
        table_name (str):
        body (RowsAggregatePlanRequest | RowsJoinPlanRequest | RowsLateralPlanRequest |
            RowsQueryPlanRequest | RowsWindowPlanRequest): Generic typed relational row plan envelope.
            It is exactly one
            operation-specific envelope: query, aggregate, window, join, or
            lateral. Query, aggregate, and window plans use `ranges`; join and
            lateral plans use paired `left_ranges` and `right_ranges`.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsAggregateResultSet | RowsQueryResultSet | RowsStreamResultSet
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
