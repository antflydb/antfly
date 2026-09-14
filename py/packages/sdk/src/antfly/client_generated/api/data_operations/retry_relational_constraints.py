from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.relational_constraint_retry_request import RelationalConstraintRetryRequest
from ...models.relational_constraint_retry_response import RelationalConstraintRetryResponse
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RelationalConstraintRetryRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/constraints/retry".format(
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
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: RelationalConstraintRetryRequest,
) -> Response[Any | Error | RelationalConstraintRetryResponse]:
    """Restart failed UNIQUE/FK validation after administrative repair

     Requires table administrator permission. Resets each failed owner using
    an exact checkpoint precondition. Owners already validating or enforced
    are unchanged. Retrying after partial progress is safe. Inspect the
    constraint status endpoint for coverage and diagnostics.
    When a retirement job is active, clears its paused diagnostic and
    resumes that job instead of restarting activation. Retirement remains
    fenced and retains all prior drain progress.

    Args:
        table_name (str):
        body (RelationalConstraintRetryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintRetryResponse]
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
    body: RelationalConstraintRetryRequest,
) -> Any | Error | RelationalConstraintRetryResponse | None:
    """Restart failed UNIQUE/FK validation after administrative repair

     Requires table administrator permission. Resets each failed owner using
    an exact checkpoint precondition. Owners already validating or enforced
    are unchanged. Retrying after partial progress is safe. Inspect the
    constraint status endpoint for coverage and diagnostics.
    When a retirement job is active, clears its paused diagnostic and
    resumes that job instead of restarting activation. Retirement remains
    fenced and retains all prior drain progress.

    Args:
        table_name (str):
        body (RelationalConstraintRetryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintRetryResponse
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
    body: RelationalConstraintRetryRequest,
) -> Response[Any | Error | RelationalConstraintRetryResponse]:
    """Restart failed UNIQUE/FK validation after administrative repair

     Requires table administrator permission. Resets each failed owner using
    an exact checkpoint precondition. Owners already validating or enforced
    are unchanged. Retrying after partial progress is safe. Inspect the
    constraint status endpoint for coverage and diagnostics.
    When a retirement job is active, clears its paused diagnostic and
    resumes that job instead of restarting activation. Retirement remains
    fenced and retains all prior drain progress.

    Args:
        table_name (str):
        body (RelationalConstraintRetryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | RelationalConstraintRetryResponse]
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
    body: RelationalConstraintRetryRequest,
) -> Any | Error | RelationalConstraintRetryResponse | None:
    """Restart failed UNIQUE/FK validation after administrative repair

     Requires table administrator permission. Resets each failed owner using
    an exact checkpoint precondition. Owners already validating or enforced
    are unchanged. Retrying after partial progress is safe. Inspect the
    constraint status endpoint for coverage and diagnostics.
    When a retirement job is active, clears its paused diagnostic and
    resumes that job instead of restarting activation. Retirement remains
    fenced and retains all prior drain progress.

    Args:
        table_name (str):
        body (RelationalConstraintRetryRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | RelationalConstraintRetryResponse
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
