from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.committed_mutation_outcome import CommittedMutationOutcome
from ...models.error import Error
from ...models.restore_job import RestoreJob
from ...models.table import Table
from ...models.table_schema import TableSchema
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: TableSchema,
    rewrite: bool | Unset = False,
    if_match: str | Unset = UNSET,
    idempotency_key: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(if_match, Unset):
        headers["If-Match"] = if_match

    if not isinstance(idempotency_key, Unset):
        headers["Idempotency-Key"] = idempotency_key

    params: dict[str, Any] = {}

    params["rewrite"] = rewrite

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/tables/{table_name}/schema".format(
            table_name=quote(str(table_name), safe=""),
        ),
        "params": params,
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> CommittedMutationOutcome | RestoreJob | Error | Table | None:
    if response.status_code == 200:
        response_200 = Table.from_dict(response.json())

        return response_200

    if response.status_code == 202:

        def _parse_response_202(data: object) -> CommittedMutationOutcome | RestoreJob:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_202_type_0 = RestoreJob.from_dict(data)

                return response_202_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_202_type_1 = CommittedMutationOutcome.from_dict(data)

            return response_202_type_1

        response_202 = _parse_response_202(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[CommittedMutationOutcome | RestoreJob | Error | Table]:
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
    body: TableSchema,
    rewrite: bool | Unset = False,
    if_match: str | Unset = UNSET,
    idempotency_key: str | Unset = UNSET,
) -> Response[CommittedMutationOutcome | RestoreJob | Error | Table]:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        rewrite (bool | Unset):  Default: False.
        if_match (str | Unset):  Example: "schema-0".
        idempotency_key (str | Unset):
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CommittedMutationOutcome | RestoreJob | Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        rewrite=rewrite,
        if_match=if_match,
        idempotency_key=idempotency_key,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    rewrite: bool | Unset = False,
    if_match: str | Unset = UNSET,
    idempotency_key: str | Unset = UNSET,
) -> CommittedMutationOutcome | RestoreJob | Error | Table | None:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        rewrite (bool | Unset):  Default: False.
        if_match (str | Unset):  Example: "schema-0".
        idempotency_key (str | Unset):
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CommittedMutationOutcome | RestoreJob | Error | Table
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
        rewrite=rewrite,
        if_match=if_match,
        idempotency_key=idempotency_key,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    rewrite: bool | Unset = False,
    if_match: str | Unset = UNSET,
    idempotency_key: str | Unset = UNSET,
) -> Response[CommittedMutationOutcome | RestoreJob | Error | Table]:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        rewrite (bool | Unset):  Default: False.
        if_match (str | Unset):  Example: "schema-0".
        idempotency_key (str | Unset):
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[CommittedMutationOutcome | RestoreJob | Error | Table]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        rewrite=rewrite,
        if_match=if_match,
        idempotency_key=idempotency_key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: TableSchema,
    rewrite: bool | Unset = False,
    if_match: str | Unset = UNSET,
    idempotency_key: str | Unset = UNSET,
) -> CommittedMutationOutcome | RestoreJob | Error | Table | None:
    """Replace a table's schema

     Replaces the complete table schema. Properties omitted from the request
    are removed. Use PATCH on this path for a partial JSON Merge Patch update.

    Args:
        table_name (str):
        rewrite (bool | Unset):  Default: False.
        if_match (str | Unset):  Example: "schema-0".
        idempotency_key (str | Unset):
        body (TableSchema): Schema definition for a table with multiple document types

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        CommittedMutationOutcome | RestoreJob | Error | Table
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
            rewrite=rewrite,
            if_match=if_match,
            idempotency_key=idempotency_key,
        )
    ).parsed
