from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.relational_index_repair_job_record import RelationalIndexRepairJobRecord
from ...types import Response


def _get_kwargs(
    table_name: str,
    job_id: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/relational-column-backed-index-repair/jobs/{job_id}".format(
            table_name=quote(str(table_name), safe=""),
            job_id=quote(str(job_id), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RelationalIndexRepairJobRecord | None:
    if response.status_code == 200:
        response_200 = RelationalIndexRepairJobRecord.from_dict(response.json())

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
) -> Response[Error | RelationalIndexRepairJobRecord]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | RelationalIndexRepairJobRecord]:
    """Get relational index repair job status

     Reads durable relational column-backed index repair job progress for the default public table.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RelationalIndexRepairJobRecord]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Error | RelationalIndexRepairJobRecord | None:
    """Get relational index repair job status

     Reads durable relational column-backed index repair job progress for the default public table.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RelationalIndexRepairJobRecord
    """

    return sync_detailed(
        table_name=table_name,
        job_id=job_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Response[Error | RelationalIndexRepairJobRecord]:
    """Get relational index repair job status

     Reads durable relational column-backed index repair job progress for the default public table.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RelationalIndexRepairJobRecord]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
) -> Error | RelationalIndexRepairJobRecord | None:
    """Get relational index repair job status

     Reads durable relational column-backed index repair job progress for the default public table.

    Args:
        table_name (str):
        job_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RelationalIndexRepairJobRecord
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            job_id=job_id,
            client=client,
        )
    ).parsed
