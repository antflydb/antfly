from http import HTTPStatus
from typing import Any, cast

import httpx

from ....sql_transport import sql_request, sql_request_async
from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.sql_diagnostic import SQLDiagnostic
from ...models.sql_request import SQLRequest
from ...models.sql_response import SQLResponse
from ...types import Response


def _get_kwargs(
    *,
    body: SQLRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/sql",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse | None:
    if response.status_code == 200:
        response_200 = SQLResponse.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = SQLResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = SQLDiagnostic.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 409:

        def _parse_response_409(data: object) -> SQLDiagnostic | SQLResponse:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_409_type_0 = SQLDiagnostic.from_dict(data)

                return response_409_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_409_type_1 = SQLResponse.from_dict(data)

            return response_409_type_1

        response_409 = _parse_response_409(response.json())

        return response_409

    if response.status_code == 429:
        response_429 = cast(Any, None)
        return response_429

    if response.status_code == 500:
        response_500 = SQLDiagnostic.from_dict(response.json())

        return response_500

    if response.status_code == 501:
        response_501 = SQLDiagnostic.from_dict(response.json())

        return response_501

    if response.status_code == 503:
        response_503 = SQLDiagnostic.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLRequest,
) -> Response[Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse]:
    """Execute a SQL statement

     Executes the bounded relational SELECT, INSERT, UPDATE, and DELETE
    subset through the current catalog and native row execution contracts.
    Parameters are bound separately from statement text. Document, graph,
    and lake SQL sources, DDL, and SQL sessions are not yet supported.
    Reads inherit native owner-local consistency, not a global SQL
    transaction snapshot. Statements requiring a retained multi-page
    statement snapshot fail when that capability is unavailable.
    Unsupported statement shapes fail explicitly; the server never
    silently substitutes a different query or truncates the result.

    Args:
        body (SQLRequest): Execute one SQL statement. Parameters are positional (`$1`, `$2`, ...),
            never interpolated into SQL text. To preserve integer precision in
            JavaScript clients, supply integers outside the exact JSON number range
            as decimal strings; binding coerces parameters to the expected type.
            The result limit is an admission bound, not an implicit SQL LIMIT:
            statements whose results exceed it fail instead of silently truncating.
            Request bodies are limited to 4 MiB, preparation to 8 MiB of allocated
            memory, and encoded results to a 16 MiB allocation budget.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = sql_request(
        client.get_httpx_client(),
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: SQLRequest,
) -> Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse | None:
    """Execute a SQL statement

     Executes the bounded relational SELECT, INSERT, UPDATE, and DELETE
    subset through the current catalog and native row execution contracts.
    Parameters are bound separately from statement text. Document, graph,
    and lake SQL sources, DDL, and SQL sessions are not yet supported.
    Reads inherit native owner-local consistency, not a global SQL
    transaction snapshot. Statements requiring a retained multi-page
    statement snapshot fail when that capability is unavailable.
    Unsupported statement shapes fail explicitly; the server never
    silently substitutes a different query or truncates the result.

    Args:
        body (SQLRequest): Execute one SQL statement. Parameters are positional (`$1`, `$2`, ...),
            never interpolated into SQL text. To preserve integer precision in
            JavaScript clients, supply integers outside the exact JSON number range
            as decimal strings; binding coerces parameters to the expected type.
            The result limit is an admission bound, not an implicit SQL LIMIT:
            statements whose results exceed it fail instead of silently truncating.
            Request bodies are limited to 4 MiB, preparation to 8 MiB of allocated
            memory, and encoded results to a 16 MiB allocation budget.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: SQLRequest,
) -> Response[Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse]:
    """Execute a SQL statement

     Executes the bounded relational SELECT, INSERT, UPDATE, and DELETE
    subset through the current catalog and native row execution contracts.
    Parameters are bound separately from statement text. Document, graph,
    and lake SQL sources, DDL, and SQL sessions are not yet supported.
    Reads inherit native owner-local consistency, not a global SQL
    transaction snapshot. Statements requiring a retained multi-page
    statement snapshot fail when that capability is unavailable.
    Unsupported statement shapes fail explicitly; the server never
    silently substitutes a different query or truncates the result.

    Args:
        body (SQLRequest): Execute one SQL statement. Parameters are positional (`$1`, `$2`, ...),
            never interpolated into SQL text. To preserve integer precision in
            JavaScript clients, supply integers outside the exact JSON number range
            as decimal strings; binding coerces parameters to the expected type.
            The result limit is an admission bound, not an implicit SQL LIMIT:
            statements whose results exceed it fail instead of silently truncating.
            Request bodies are limited to 4 MiB, preparation to 8 MiB of allocated
            memory, and encoded results to a 16 MiB allocation budget.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await sql_request_async(client.get_async_httpx_client(), **kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: SQLRequest,
) -> Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse | None:
    """Execute a SQL statement

     Executes the bounded relational SELECT, INSERT, UPDATE, and DELETE
    subset through the current catalog and native row execution contracts.
    Parameters are bound separately from statement text. Document, graph,
    and lake SQL sources, DDL, and SQL sessions are not yet supported.
    Reads inherit native owner-local consistency, not a global SQL
    transaction snapshot. Statements requiring a retained multi-page
    statement snapshot fail when that capability is unavailable.
    Unsupported statement shapes fail explicitly; the server never
    silently substitutes a different query or truncates the result.

    Args:
        body (SQLRequest): Execute one SQL statement. Parameters are positional (`$1`, `$2`, ...),
            never interpolated into SQL text. To preserve integer precision in
            JavaScript clients, supply integers outside the exact JSON number range
            as decimal strings; binding coerces parameters to the expected type.
            The result limit is an admission bound, not an implicit SQL LIMIT:
            statements whose results exceed it fail instead of silently truncating.
            Request bodies are limited to 4 MiB, preparation to 8 MiB of allocated
            memory, and encoded results to a 16 MiB allocation budget.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | SQLDiagnostic | SQLDiagnostic | SQLResponse | SQLResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
