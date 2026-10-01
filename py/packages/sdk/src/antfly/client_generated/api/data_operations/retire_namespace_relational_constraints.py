from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.relational_constraint_retirement_request import RelationalConstraintRetirementRequest
from ...models.relational_constraint_retry_response import RelationalConstraintRetryResponse
from ...types import Response


def _get_kwargs(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    body: RelationalConstraintRetirementRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/databases/{database_name}/namespaces/{namespace_name}/tables/{table_name}/constraints/retire".format(
            database_name=quote(str(database_name), safe=""),
            namespace_name=quote(str(namespace_name), safe=""),
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | RelationalConstraintRetryResponse | None:
    if response.status_code == 202:
        response_202 = RelationalConstraintRetryResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | Error | RelationalConstraintRetryResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RelationalConstraintRetirementRequest,
) -> Response[Any | Error | RelationalConstraintRetryResponse]:
    """Retire unique and foreign-key definitions safely

     Requires table administrator permission. Starts a durable, bounded
    all-owner drain. Poll constraints/status for progress. A target schema
    is published automatically after the drain. With drop=true the table
    remains fenced at ready_to_drop until an administrator explicitly
    deletes it with the existing table deletion endpoint.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RelationalConstraintRetirementRequest): Supply exactly one of target_schema or
            drop=true. A target schema may
            only remove UNIQUE/FK definitions; all other schema properties must
            remain unchanged. Its version is assigned by the server. Retirement
            fences primary mutations while existing reference and claim records
            are drained. External foreign keys referencing removed definitions
            must be retired first.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintRetryResponse]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RelationalConstraintRetirementRequest,
) -> Any | Error | RelationalConstraintRetryResponse | None:
    """Retire unique and foreign-key definitions safely

     Requires table administrator permission. Starts a durable, bounded
    all-owner drain. Poll constraints/status for progress. A target schema
    is published automatically after the drain. With drop=true the table
    remains fenced at ready_to_drop until an administrator explicitly
    deletes it with the existing table deletion endpoint.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RelationalConstraintRetirementRequest): Supply exactly one of target_schema or
            drop=true. A target schema may
            only remove UNIQUE/FK definitions; all other schema properties must
            remain unchanged. Its version is assigned by the server. Retirement
            fences primary mutations while existing reference and claim records
            are drained. External foreign keys referencing removed definitions
            must be retired first.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintRetryResponse
    """

    return sync_detailed(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RelationalConstraintRetirementRequest,
) -> Response[Any | Error | RelationalConstraintRetryResponse]:
    """Retire unique and foreign-key definitions safely

     Requires table administrator permission. Starts a durable, bounded
    all-owner drain. Poll constraints/status for progress. A target schema
    is published automatically after the drain. With drop=true the table
    remains fenced at ready_to_drop until an administrator explicitly
    deletes it with the existing table deletion endpoint.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RelationalConstraintRetirementRequest): Supply exactly one of target_schema or
            drop=true. A target schema may
            only remove UNIQUE/FK definitions; all other schema properties must
            remain unchanged. Its version is assigned by the server. Retirement
            fences primary mutations while existing reference and claim records
            are drained. External foreign keys referencing removed definitions
            must be retired first.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintRetryResponse]
    """

    kwargs = _get_kwargs(
        database_name=database_name,
        namespace_name=namespace_name,
        table_name=table_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    database_name: str,
    namespace_name: str,
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RelationalConstraintRetirementRequest,
) -> Any | Error | RelationalConstraintRetryResponse | None:
    """Retire unique and foreign-key definitions safely

     Requires table administrator permission. Starts a durable, bounded
    all-owner drain. Poll constraints/status for progress. A target schema
    is published automatically after the drain. With drop=true the table
    remains fenced at ready_to_drop until an administrator explicitly
    deletes it with the existing table deletion endpoint.

    Args:
        database_name (str):
        namespace_name (str):
        table_name (str):
        body (RelationalConstraintRetirementRequest): Supply exactly one of target_schema or
            drop=true. A target schema may
            only remove UNIQUE/FK definitions; all other schema properties must
            remain unchanged. Its version is assigned by the server. Retirement
            fences primary mutations while existing reference and claim records
            are drained. External foreign keys referencing removed definitions
            must be retired first.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintRetryResponse
    """

    return (
        await asyncio_detailed(
            database_name=database_name,
            namespace_name=namespace_name,
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
