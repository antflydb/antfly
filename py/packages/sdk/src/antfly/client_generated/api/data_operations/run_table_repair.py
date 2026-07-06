from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.artifact_repair_run_response import ArtifactRepairRunResponse
from ...models.error import Error
from ...models.repair_run_request import RepairRunRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: RepairRunRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/repair/run".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if not isinstance(body, Unset):
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ArtifactRepairRunResponse | Error | None:
    if response.status_code == 202:
        response_202 = ArtifactRepairRunResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 405:
        response_405 = Error.from_dict(response.json())

        return response_405

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[ArtifactRepairRunResponse | Error]:
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
    body: RepairRunRequest | Unset = UNSET,
) -> Response[ArtifactRepairRunResponse | Error]:
    """Run a bounded table repair pass

     Attempts to repair queued table issues. `target=artifact` reprocesses
    supported artifact kinds and replays derived state; it is bounded by
    `limit` and returns an opaque continuation cursor when another artifact
    repair page is available. `target=index` repairs one named index after
    resetting its derived index storage; healthy indexes are skipped unless
    `force=true` is supplied, and any positive `limit` permits that single
    named index repair. The response reports unresolved debt separately, and
    the endpoint requires table admin permission when authentication is
    enabled.

    Args:
        table_name (str):
        body (RepairRunRequest | Unset): Bounded request to run a table repair pass.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactRepairRunResponse | Error]
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
    body: RepairRunRequest | Unset = UNSET,
) -> ArtifactRepairRunResponse | Error | None:
    """Run a bounded table repair pass

     Attempts to repair queued table issues. `target=artifact` reprocesses
    supported artifact kinds and replays derived state; it is bounded by
    `limit` and returns an opaque continuation cursor when another artifact
    repair page is available. `target=index` repairs one named index after
    resetting its derived index storage; healthy indexes are skipped unless
    `force=true` is supplied, and any positive `limit` permits that single
    named index repair. The response reports unresolved debt separately, and
    the endpoint requires table admin permission when authentication is
    enabled.

    Args:
        table_name (str):
        body (RepairRunRequest | Unset): Bounded request to run a table repair pass.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactRepairRunResponse | Error
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
    body: RepairRunRequest | Unset = UNSET,
) -> Response[ArtifactRepairRunResponse | Error]:
    """Run a bounded table repair pass

     Attempts to repair queued table issues. `target=artifact` reprocesses
    supported artifact kinds and replays derived state; it is bounded by
    `limit` and returns an opaque continuation cursor when another artifact
    repair page is available. `target=index` repairs one named index after
    resetting its derived index storage; healthy indexes are skipped unless
    `force=true` is supplied, and any positive `limit` permits that single
    named index repair. The response reports unresolved debt separately, and
    the endpoint requires table admin permission when authentication is
    enabled.

    Args:
        table_name (str):
        body (RepairRunRequest | Unset): Bounded request to run a table repair pass.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactRepairRunResponse | Error]
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
    body: RepairRunRequest | Unset = UNSET,
) -> ArtifactRepairRunResponse | Error | None:
    """Run a bounded table repair pass

     Attempts to repair queued table issues. `target=artifact` reprocesses
    supported artifact kinds and replays derived state; it is bounded by
    `limit` and returns an opaque continuation cursor when another artifact
    repair page is available. `target=index` repairs one named index after
    resetting its derived index storage; healthy indexes are skipped unless
    `force=true` is supplied, and any positive `limit` permits that single
    named index repair. The response reports unresolved debt separately, and
    the endpoint requires table admin permission when authentication is
    enabled.

    Args:
        table_name (str):
        body (RepairRunRequest | Unset): Bounded request to run a table repair pass.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactRepairRunResponse | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
