from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_capacity_error import InferenceCapacityError
from ...models.invoke_inference_connection_body import InvokeInferenceConnectionBody
from ...models.invoke_inference_connection_operation import InvokeInferenceConnectionOperation
from ...types import Response


def _get_kwargs(
    connection_id: str,
    operation: InvokeInferenceConnectionOperation,
    *,
    body: InvokeInferenceConnectionBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/connections/{connection_id}/inference/{operation}".format(
            connection_id=quote(str(connection_id), safe=""),
            operation=quote(str(operation), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | InferenceCapacityError | str | None:
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 401:
        response_401 = cast(Any, None)
        return response_401

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 502:
        response_502 = cast(Any, None)
        return response_502

    if response.status_code == 503:
        response_503 = InferenceCapacityError.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | InferenceCapacityError | str]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    connection_id: str,
    operation: InvokeInferenceConnectionOperation,
    *,
    client: AuthenticatedClient,
    body: InvokeInferenceConnectionBody,
) -> Response[Any | InferenceCapacityError | str]:
    """Invoke an Antfly-compatible inference connection

     Invokes an inference operation through the selected connection.
    Requires `inference/*` write permission. Generation requests with
    `stream: true` return the provider's Server-Sent Events stream; all
    other responses are returned as buffered JSON.

    Args:
        connection_id (str):
        operation (InvokeInferenceConnectionOperation):
        body (InvokeInferenceConnectionBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | InferenceCapacityError | str]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        operation=operation,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    connection_id: str,
    operation: InvokeInferenceConnectionOperation,
    *,
    client: AuthenticatedClient,
    body: InvokeInferenceConnectionBody,
) -> Any | InferenceCapacityError | str | None:
    """Invoke an Antfly-compatible inference connection

     Invokes an inference operation through the selected connection.
    Requires `inference/*` write permission. Generation requests with
    `stream: true` return the provider's Server-Sent Events stream; all
    other responses are returned as buffered JSON.

    Args:
        connection_id (str):
        operation (InvokeInferenceConnectionOperation):
        body (InvokeInferenceConnectionBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | InferenceCapacityError | str
    """

    return sync_detailed(
        connection_id=connection_id,
        operation=operation,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    connection_id: str,
    operation: InvokeInferenceConnectionOperation,
    *,
    client: AuthenticatedClient,
    body: InvokeInferenceConnectionBody,
) -> Response[Any | InferenceCapacityError | str]:
    """Invoke an Antfly-compatible inference connection

     Invokes an inference operation through the selected connection.
    Requires `inference/*` write permission. Generation requests with
    `stream: true` return the provider's Server-Sent Events stream; all
    other responses are returned as buffered JSON.

    Args:
        connection_id (str):
        operation (InvokeInferenceConnectionOperation):
        body (InvokeInferenceConnectionBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | InferenceCapacityError | str]
    """

    kwargs = _get_kwargs(
        connection_id=connection_id,
        operation=operation,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    connection_id: str,
    operation: InvokeInferenceConnectionOperation,
    *,
    client: AuthenticatedClient,
    body: InvokeInferenceConnectionBody,
) -> Any | InferenceCapacityError | str | None:
    """Invoke an Antfly-compatible inference connection

     Invokes an inference operation through the selected connection.
    Requires `inference/*` write permission. Generation requests with
    `stream: true` return the provider's Server-Sent Events stream; all
    other responses are returned as buffered JSON.

    Args:
        connection_id (str):
        operation (InvokeInferenceConnectionOperation):
        body (InvokeInferenceConnectionBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | InferenceCapacityError | str
    """

    return (
        await asyncio_detailed(
            connection_id=connection_id,
            operation=operation,
            client=client,
            body=body,
        )
    ).parsed
