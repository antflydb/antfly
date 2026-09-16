from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.advance_table_storage_migration_body import AdvanceTableStorageMigrationBody
from ...models.advance_table_storage_migration_response_200 import AdvanceTableStorageMigrationResponse200
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    job_id: str,
    *,
    body: AdvanceTableStorageMigrationBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/storage/migrations/{job_id}".format(
            table_name=quote(str(table_name), safe=""),
            job_id=quote(str(job_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> AdvanceTableStorageMigrationResponse200 | Any | Error | None:
    if response.status_code == 200:
        response_200 = AdvanceTableStorageMigrationResponse200.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[AdvanceTableStorageMigrationResponse200 | Any | Error]:
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
    body: AdvanceTableStorageMigrationBody,
) -> Response[AdvanceTableStorageMigrationResponse200 | Any | Error]:
    """Advance, publish or cancel a table storage migration job

     Uses the job's durable configuration and budgets. Each step commits
    bounded progress. Publish is accepted only at ready; complete additionally
    certifies reference-only primary artifacts and native ANN serving.
    Cancellation is allowed only before publication. Repeating an action
    after an ambiguous response resumes the durable job.

    Args:
        table_name (str):
        job_id (str):
        body (AdvanceTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AdvanceTableStorageMigrationResponse200 | Any | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
        body=body,
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
    body: AdvanceTableStorageMigrationBody,
) -> AdvanceTableStorageMigrationResponse200 | Any | Error | None:
    """Advance, publish or cancel a table storage migration job

     Uses the job's durable configuration and budgets. Each step commits
    bounded progress. Publish is accepted only at ready; complete additionally
    certifies reference-only primary artifacts and native ANN serving.
    Cancellation is allowed only before publication. Repeating an action
    after an ambiguous response resumes the durable job.

    Args:
        table_name (str):
        job_id (str):
        body (AdvanceTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AdvanceTableStorageMigrationResponse200 | Any | Error
    """

    return sync_detailed(
        table_name=table_name,
        job_id=job_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: AdvanceTableStorageMigrationBody,
) -> Response[AdvanceTableStorageMigrationResponse200 | Any | Error]:
    """Advance, publish or cancel a table storage migration job

     Uses the job's durable configuration and budgets. Each step commits
    bounded progress. Publish is accepted only at ready; complete additionally
    certifies reference-only primary artifacts and native ANN serving.
    Cancellation is allowed only before publication. Repeating an action
    after an ambiguous response resumes the durable job.

    Args:
        table_name (str):
        job_id (str):
        body (AdvanceTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[AdvanceTableStorageMigrationResponse200 | Any | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        job_id=job_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    job_id: str,
    *,
    client: AuthenticatedClient,
    body: AdvanceTableStorageMigrationBody,
) -> AdvanceTableStorageMigrationResponse200 | Any | Error | None:
    """Advance, publish or cancel a table storage migration job

     Uses the job's durable configuration and budgets. Each step commits
    bounded progress. Publish is accepted only at ready; complete additionally
    certifies reference-only primary artifacts and native ANN serving.
    Cancellation is allowed only before publication. Repeating an action
    after an ambiguous response resumes the durable job.

    Args:
        table_name (str):
        job_id (str):
        body (AdvanceTableStorageMigrationBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        AdvanceTableStorageMigrationResponse200 | Any | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            job_id=job_id,
            client=client,
            body=body,
        )
    ).parsed
