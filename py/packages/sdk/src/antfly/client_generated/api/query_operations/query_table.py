from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.exact_sort_error import ExactSortError
from ...models.graph_anchor_filter_requires_index_error import GraphAnchorFilterRequiresIndexError
from ...models.graph_distinct_budget_exceeded_error import GraphDistinctBudgetExceededError
from ...models.graph_match_operation_limit_exceeded_error import GraphMatchOperationLimitExceededError
from ...models.graph_path_weight_domain_error import GraphPathWeightDomainError
from ...models.graph_query_mode_unsupported_error import GraphQueryModeUnsupportedError
from ...models.graph_work_budget_exceeded_error import GraphWorkBudgetExceededError
from ...models.hierarchy_cursor_stale_error import HierarchyCursorStaleError
from ...models.query_candidate_budget_exceeded_error import QueryCandidateBudgetExceededError
from ...models.query_request import QueryRequest
from ...models.query_responses import QueryResponses
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...models.table_storage_unreadable_error import TableStorageUnreadableError
from ...models.topology_changed_error import TopologyChangedError
from ...types import File, Response


def _get_kwargs(
    table_name: str,
    *,
    body: QueryRequest | File,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/query".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if isinstance(body, QueryRequest):
        _kwargs["json"] = body.to_dict()

        headers["Content-Type"] = "application/json"
    if isinstance(body, File):
        _kwargs["content"] = body.payload

        headers["Content-Type"] = "application/x-ndjson"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    if response.status_code == 200:
        response_200 = QueryResponses.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:

        def _parse_response_409(data: object) -> HierarchyCursorStaleError | TopologyChangedError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_conflict_error_type_0 = HierarchyCursorStaleError.from_dict(data)

                return componentsschemas_query_conflict_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_conflict_error_type_1 = TopologyChangedError.from_dict(data)

            return componentsschemas_query_conflict_error_type_1

        response_409 = _parse_response_409(response.json())

        return response_409

    if response.status_code == 422:

        def _parse_response_422(
            data: object,
        ) -> (
            ExactSortError
            | GraphAnchorFilterRequiresIndexError
            | GraphDistinctBudgetExceededError
            | GraphMatchOperationLimitExceededError
            | GraphPathWeightDomainError
            | GraphQueryModeUnsupportedError
            | GraphWorkBudgetExceededError
            | QueryCandidateBudgetExceededError
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_0 = ExactSortError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_1 = QueryCandidateBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_2 = GraphDistinctBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_3 = GraphWorkBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_4 = GraphPathWeightDomainError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_5 = GraphAnchorFilterRequiresIndexError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_6 = GraphQueryModeUnsupportedError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_unprocessable_error_type_7 = GraphMatchOperationLimitExceededError.from_dict(data)

            return componentsschemas_query_unprocessable_error_type_7

        response_422 = _parse_response_422(response.json())

        return response_422

    if response.status_code == 500:

        def _parse_response_500(data: object) -> Error | TableStorageUnreadableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_500_type_0 = Error.from_dict(data)

                return response_500_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_500_type_1 = TableStorageUnreadableError.from_dict(data)

            return response_500_type_1

        response_500 = _parse_response_500(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = QueryTemporarilyUnavailableError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
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
    body: QueryRequest | File,
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
    """Query a specific table

    Args:
        table_name (str):
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryModeUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | TopologyChangedError | QueryResponses | QueryTemporarilyUnavailableError]
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
    body: QueryRequest | File,
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    """Query a specific table

    Args:
        table_name (str):
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryModeUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | TopologyChangedError | QueryResponses | QueryTemporarilyUnavailableError
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
    body: QueryRequest | File,
) -> Response[
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
]:
    """Query a specific table

    Args:
        table_name (str):
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryModeUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | TopologyChangedError | QueryResponses | QueryTemporarilyUnavailableError]
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
    body: QueryRequest | File,
) -> (
    Error
    | Error
    | TableStorageUnreadableError
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryModeUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | HierarchyCursorStaleError
    | TopologyChangedError
    | QueryResponses
    | QueryTemporarilyUnavailableError
    | None
):
    """Query a specific table

    Args:
        table_name (str):
        body (QueryRequest):
        body (File):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | TableStorageUnreadableError | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryModeUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | HierarchyCursorStaleError | TopologyChangedError | QueryResponses | QueryTemporarilyUnavailableError
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
