from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.multi_batch_request import MultiBatchRequest
from ...models.multi_batch_response import MultiBatchResponse
from ...models.transaction_commit_response import TransactionCommitResponse
from ...models.transaction_write_error import TransactionWriteError
from ...types import Response


def _get_kwargs(
    *,
    body: MultiBatchRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/batch",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str | None:
    if response.status_code == 201:
        response_201 = MultiBatchResponse.from_dict(response.json())

        return response_201

    if response.status_code == 202:
        response_202 = MultiBatchResponse.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        if response.headers.get("content-type", "").partition(";")[0].strip().lower() == "text/plain":
            return response.text

        def _parse_response_403(data: object) -> Error | TransactionWriteError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_403_type_0 = TransactionWriteError.from_dict(data)

                return response_403_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_403_type_1 = Error.from_dict(data)

            return response_403_type_1

        response_403 = _parse_response_403(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        if response.headers.get("content-type", "").partition(";")[0].strip().lower() == "text/plain":
            return response.text
        response_409 = TransactionCommitResponse.from_dict(response.json())

        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        if response.headers.get("content-type", "").partition(";")[0].strip().lower() == "text/plain":
            return response.text

        def _parse_response_503(data: object) -> Error | TransactionWriteError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_503_type_0 = TransactionWriteError.from_dict(data)

                return response_503_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_503_type_1 = Error.from_dict(data)

            return response_503_type_1

        response_503 = _parse_response_503(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: MultiBatchRequest,
) -> Response[Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str]:
    """Cross-table batch operations

     Perform batch inserts, deletes, and transforms across multiple tables
    in a single atomic transaction.

    All operations across all tables are committed atomically using distributed
    2-phase commit (2PC). Either all operations succeed, or none do.

    **Use cases**:
    - Transfer records between tables (insert in one, delete from another)
    - Maintain referential integrity across tables
    - Atomic multi-table updates

    Args:
        body (MultiBatchRequest): Cross-table batch operations in a single atomic transaction.

            Groups batch operations by table name. All operations across all tables
            are committed atomically using distributed 2-phase commit (2PC).

            **Atomicity**: Either all operations across all tables succeed, or none do.
            This enables use cases like transferring a record from one table to another,
            or maintaining referential integrity across tables.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: MultiBatchRequest,
) -> Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str | None:
    """Cross-table batch operations

     Perform batch inserts, deletes, and transforms across multiple tables
    in a single atomic transaction.

    All operations across all tables are committed atomically using distributed
    2-phase commit (2PC). Either all operations succeed, or none do.

    **Use cases**:
    - Transfer records between tables (insert in one, delete from another)
    - Maintain referential integrity across tables
    - Atomic multi-table updates

    Args:
        body (MultiBatchRequest): Cross-table batch operations in a single atomic transaction.

            Groups batch operations by table name. All operations across all tables
            are committed atomically using distributed 2-phase commit (2PC).

            **Atomicity**: Either all operations across all tables succeed, or none do.
            This enables use cases like transferring a record from one table to another,
            or maintaining referential integrity across tables.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: MultiBatchRequest,
) -> Response[Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str]:
    """Cross-table batch operations

     Perform batch inserts, deletes, and transforms across multiple tables
    in a single atomic transaction.

    All operations across all tables are committed atomically using distributed
    2-phase commit (2PC). Either all operations succeed, or none do.

    **Use cases**:
    - Transfer records between tables (insert in one, delete from another)
    - Maintain referential integrity across tables
    - Atomic multi-table updates

    Args:
        body (MultiBatchRequest): Cross-table batch operations in a single atomic transaction.

            Groups batch operations by table name. All operations across all tables
            are committed atomically using distributed 2-phase commit (2PC).

            **Atomicity**: Either all operations across all tables succeed, or none do.
            This enables use cases like transferring a record from one table to another,
            or maintaining referential integrity across tables.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: MultiBatchRequest,
) -> Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str | None:
    """Cross-table batch operations

     Perform batch inserts, deletes, and transforms across multiple tables
    in a single atomic transaction.

    All operations across all tables are committed atomically using distributed
    2-phase commit (2PC). Either all operations succeed, or none do.

    **Use cases**:
    - Transfer records between tables (insert in one, delete from another)
    - Maintain referential integrity across tables
    - Atomic multi-table updates

    Args:
        body (MultiBatchRequest): Cross-table batch operations in a single atomic transaction.

            Groups batch operations by table name. All operations across all tables
            are committed atomically using distributed 2-phase commit (2PC).

            **Atomicity**: Either all operations across all tables succeed, or none do.
            This enables use cases like transferring a record from one table to another,
            or maintaining referential integrity across tables.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | TransactionWriteError | MultiBatchResponse | TransactionCommitResponse | str
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
