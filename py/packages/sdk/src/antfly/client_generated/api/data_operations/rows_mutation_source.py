from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.rows_insert_source_request import RowsInsertSourceRequest
from ...models.rows_joined_mutation_source_request import RowsJoinedMutationSourceRequest
from ...models.rows_mutation_source_request import RowsMutationSourceRequest
from ...models.rows_mutation_source_result_set import RowsMutationSourceResultSet
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RowsInsertSourceRequest | RowsJoinedMutationSourceRequest | RowsMutationSourceRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/mutation-source".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    if isinstance(body, RowsMutationSourceRequest):
        _kwargs["json"] = body.to_dict()
    elif isinstance(body, RowsInsertSourceRequest):
        _kwargs["json"] = body.to_dict()
    else:
        _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | RowsMutationSourceResultSet | None:
    if response.status_code == 200:
        response_200 = RowsMutationSourceResultSet.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = Error.from_dict(response.json())

        return response_409

    if response.status_code == 501:
        response_501 = Error.from_dict(response.json())

        return response_501

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RowsMutationSourceResultSet]:
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
    body: RowsInsertSourceRequest | RowsJoinedMutationSourceRequest | RowsMutationSourceRequest,
) -> Response[Error | RowsMutationSourceResultSet]:
    """Stage typed relational update/delete operations from a claimed row source

     Transaction-staging endpoint for bounded multi-row relational writes.
    Update/delete sources use either a typed base row query with a
    `row_claim` and transaction id, or a typed joined mutation-source plan
    whose target side carries the row claim. Insert-source requests expose
    the native source-to-target insert plan shape: the server reads the
    typed source query, applies target-column assignments and conflict
    actions through the row-batch constraint path, and returns optional
    projections from the planned final target image. Update/delete plans
    claim selected target rows, record committed-version predicates, stage
    intents into the existing transaction, and return optional projections
    from the planned final target image or deleted target row image.

    Args:
        table_name (str):
        body (RowsInsertSourceRequest | RowsJoinedMutationSourceRequest |
            RowsMutationSourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsMutationSourceResultSet]
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
    body: RowsInsertSourceRequest | RowsJoinedMutationSourceRequest | RowsMutationSourceRequest,
) -> Error | RowsMutationSourceResultSet | None:
    """Stage typed relational update/delete operations from a claimed row source

     Transaction-staging endpoint for bounded multi-row relational writes.
    Update/delete sources use either a typed base row query with a
    `row_claim` and transaction id, or a typed joined mutation-source plan
    whose target side carries the row claim. Insert-source requests expose
    the native source-to-target insert plan shape: the server reads the
    typed source query, applies target-column assignments and conflict
    actions through the row-batch constraint path, and returns optional
    projections from the planned final target image. Update/delete plans
    claim selected target rows, record committed-version predicates, stage
    intents into the existing transaction, and return optional projections
    from the planned final target image or deleted target row image.

    Args:
        table_name (str):
        body (RowsInsertSourceRequest | RowsJoinedMutationSourceRequest |
            RowsMutationSourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsMutationSourceResultSet
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
    body: RowsInsertSourceRequest | RowsJoinedMutationSourceRequest | RowsMutationSourceRequest,
) -> Response[Error | RowsMutationSourceResultSet]:
    """Stage typed relational update/delete operations from a claimed row source

     Transaction-staging endpoint for bounded multi-row relational writes.
    Update/delete sources use either a typed base row query with a
    `row_claim` and transaction id, or a typed joined mutation-source plan
    whose target side carries the row claim. Insert-source requests expose
    the native source-to-target insert plan shape: the server reads the
    typed source query, applies target-column assignments and conflict
    actions through the row-batch constraint path, and returns optional
    projections from the planned final target image. Update/delete plans
    claim selected target rows, record committed-version predicates, stage
    intents into the existing transaction, and return optional projections
    from the planned final target image or deleted target row image.

    Args:
        table_name (str):
        body (RowsInsertSourceRequest | RowsJoinedMutationSourceRequest |
            RowsMutationSourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowsMutationSourceResultSet]
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
    body: RowsInsertSourceRequest | RowsJoinedMutationSourceRequest | RowsMutationSourceRequest,
) -> Error | RowsMutationSourceResultSet | None:
    """Stage typed relational update/delete operations from a claimed row source

     Transaction-staging endpoint for bounded multi-row relational writes.
    Update/delete sources use either a typed base row query with a
    `row_claim` and transaction id, or a typed joined mutation-source plan
    whose target side carries the row claim. Insert-source requests expose
    the native source-to-target insert plan shape: the server reads the
    typed source query, applies target-column assignments and conflict
    actions through the row-batch constraint path, and returns optional
    projections from the planned final target image. Update/delete plans
    claim selected target rows, record committed-version predicates, stage
    intents into the existing transaction, and return optional projections
    from the planned final target image or deleted target row image.

    Args:
        table_name (str):
        body (RowsInsertSourceRequest | RowsJoinedMutationSourceRequest |
            RowsMutationSourceRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowsMutationSourceResultSet
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
