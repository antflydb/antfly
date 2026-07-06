from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.artifact_repair_issue_list import ArtifactRepairIssueList
from ...models.error import Error
from ...models.repair_issue_list_request import RepairIssueListRequest
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: RepairIssueListRequest | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/repair/issues".format(
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
) -> ArtifactRepairIssueList | Error | None:
    if response.status_code == 200:
        response_200 = ArtifactRepairIssueList.from_dict(response.json())

        return response_200

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
) -> Response[ArtifactRepairIssueList | Error]:
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
    body: RepairIssueListRequest | Unset = UNSET,
) -> Response[ArtifactRepairIssueList | Error]:
    """List artifact repair issues

     Lists durable repair debt for a table. This operator-facing endpoint
    returns exact document keys, artifact keys, index names, and repair
    errors, and therefore requires table admin permission when authentication
    is enabled. Request filters are supplied in the JSON body. This release
    supports `target=artifact` for durable artifact queue entries and
    `target=index` for index repair candidates.

    Args:
        table_name (str):
        body (RepairIssueListRequest | Unset): Bounded request to list table repair issues.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactRepairIssueList | Error]
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
    body: RepairIssueListRequest | Unset = UNSET,
) -> ArtifactRepairIssueList | Error | None:
    """List artifact repair issues

     Lists durable repair debt for a table. This operator-facing endpoint
    returns exact document keys, artifact keys, index names, and repair
    errors, and therefore requires table admin permission when authentication
    is enabled. Request filters are supplied in the JSON body. This release
    supports `target=artifact` for durable artifact queue entries and
    `target=index` for index repair candidates.

    Args:
        table_name (str):
        body (RepairIssueListRequest | Unset): Bounded request to list table repair issues.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactRepairIssueList | Error
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
    body: RepairIssueListRequest | Unset = UNSET,
) -> Response[ArtifactRepairIssueList | Error]:
    """List artifact repair issues

     Lists durable repair debt for a table. This operator-facing endpoint
    returns exact document keys, artifact keys, index names, and repair
    errors, and therefore requires table admin permission when authentication
    is enabled. Request filters are supplied in the JSON body. This release
    supports `target=artifact` for durable artifact queue entries and
    `target=index` for index repair candidates.

    Args:
        table_name (str):
        body (RepairIssueListRequest | Unset): Bounded request to list table repair issues.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactRepairIssueList | Error]
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
    body: RepairIssueListRequest | Unset = UNSET,
) -> ArtifactRepairIssueList | Error | None:
    """List artifact repair issues

     Lists durable repair debt for a table. This operator-facing endpoint
    returns exact document keys, artifact keys, index names, and repair
    errors, and therefore requires table admin permission when authentication
    is enabled. Request filters are supplied in the JSON body. This release
    supports `target=artifact` for durable artifact queue entries and
    `target=index` for index repair candidates.

    Args:
        table_name (str):
        body (RepairIssueListRequest | Unset): Bounded request to list table repair issues.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactRepairIssueList | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
