from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.index_maintenance_request import IndexMaintenanceRequest
from ...models.index_maintenance_response import IndexMaintenanceResponse
from ...models.index_mutation_conflict_error import IndexMutationConflictError
from ...models.index_mutation_service_unavailable_error import IndexMutationServiceUnavailableError
from ...models.storage_resource_exhausted_error import StorageResourceExhaustedError
from ...types import Response


def _get_kwargs(
    table_name: str,
    index_name: str,
    *,
    body: IndexMaintenanceRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/indexes/{index_name}/retry".format(
            table_name=quote(str(table_name), safe=""),
            index_name=quote(str(index_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
    | None
):
    if response.status_code == 200:
        response_200 = IndexMaintenanceResponse.from_dict(response.json())

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

    if response.status_code == 409:
        response_409 = IndexMutationConflictError.from_dict(response.json())

        return response_409

    if response.status_code == 429:
        response_429 = StorageResourceExhaustedError.from_dict(response.json())

        return response_429

    if response.status_code == 503:

        def _parse_response_503(data: object) -> Error | IndexMutationServiceUnavailableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_503_type_0 = IndexMutationServiceUnavailableError.from_dict(data)

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
) -> Response[
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: IndexMaintenanceRequest,
) -> Response[
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
]:
    """Retry a failed index build

     Generation-fenced relational index maintenance. Requires table ADMIN permission. Retry accepts
    failed generations. Owners are admitted independently and durably; exact request replay resumes
    after partial acknowledgements. Unsupported index types return 405.

    Args:
        table_name (str):
        index_name (str):
        body (IndexMaintenanceRequest): Exact observations from index status. Each selected owner
            is admitted atomically through the replicated transaction journal; the selection is not
            one global transaction. Cancellation, conflicts, or a lost acknowledgement may leave some
            owners admitted. Resubmit the identical request to resume safely; do not replace its
            observations with newer progress unless starting a new maintenance attempt.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | IndexMutationServiceUnavailableError | IndexMaintenanceResponse | IndexMutationConflictError | StorageResourceExhaustedError]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: IndexMaintenanceRequest,
) -> (
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
    | None
):
    """Retry a failed index build

     Generation-fenced relational index maintenance. Requires table ADMIN permission. Retry accepts
    failed generations. Owners are admitted independently and durably; exact request replay resumes
    after partial acknowledgements. Unsupported index types return 405.

    Args:
        table_name (str):
        index_name (str):
        body (IndexMaintenanceRequest): Exact observations from index status. Each selected owner
            is admitted atomically through the replicated transaction journal; the selection is not
            one global transaction. Cancellation, conflicts, or a lost acknowledgement may leave some
            owners admitted. Resubmit the identical request to resume safely; do not replace its
            observations with newer progress unless starting a new maintenance attempt.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | IndexMutationServiceUnavailableError | IndexMaintenanceResponse | IndexMutationConflictError | StorageResourceExhaustedError
    """

    return sync_detailed(
        table_name=table_name,
        index_name=index_name,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: IndexMaintenanceRequest,
) -> Response[
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
]:
    """Retry a failed index build

     Generation-fenced relational index maintenance. Requires table ADMIN permission. Retry accepts
    failed generations. Owners are admitted independently and durably; exact request replay resumes
    after partial acknowledgements. Unsupported index types return 405.

    Args:
        table_name (str):
        index_name (str):
        body (IndexMaintenanceRequest): Exact observations from index status. Each selected owner
            is admitted atomically through the replicated transaction journal; the selection is not
            one global transaction. Cancellation, conflicts, or a lost acknowledgement may leave some
            owners admitted. Resubmit the identical request to resume safely; do not replace its
            observations with newer progress unless starting a new maintenance attempt.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | Error | IndexMutationServiceUnavailableError | IndexMaintenanceResponse | IndexMutationConflictError | StorageResourceExhaustedError]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        index_name=index_name,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    index_name: str,
    *,
    client: AuthenticatedClient,
    body: IndexMaintenanceRequest,
) -> (
    Error
    | Error
    | IndexMutationServiceUnavailableError
    | IndexMaintenanceResponse
    | IndexMutationConflictError
    | StorageResourceExhaustedError
    | None
):
    """Retry a failed index build

     Generation-fenced relational index maintenance. Requires table ADMIN permission. Retry accepts
    failed generations. Owners are admitted independently and durably; exact request replay resumes
    after partial acknowledgements. Unsupported index types return 405.

    Args:
        table_name (str):
        index_name (str):
        body (IndexMaintenanceRequest): Exact observations from index status. Each selected owner
            is admitted atomically through the replicated transaction journal; the selection is not
            one global transaction. Cancellation, conflicts, or a lost acknowledgement may leave some
            owners admitted. Resubmit the identical request to resume safely; do not replace its
            observations with newer progress unless starting a new maintenance attempt.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | Error | IndexMutationServiceUnavailableError | IndexMaintenanceResponse | IndexMutationConflictError | StorageResourceExhaustedError
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            index_name=index_name,
            client=client,
            body=body,
        )
    ).parsed
