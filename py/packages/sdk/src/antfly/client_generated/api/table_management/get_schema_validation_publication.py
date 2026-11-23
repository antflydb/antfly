from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.schema_validation_status import SchemaValidationStatus
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    x_antfly_schema_validation_token: str,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    headers["X-Antfly-Schema-Validation-Token"] = x_antfly_schema_validation_token

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/schema/publication".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | Error | SchemaValidationStatus | None:
    if response.status_code == 200:
        response_200 = SchemaValidationStatus.from_dict(response.json())

        return response_200

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
) -> Response[Any | Error | SchemaValidationStatus]:
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
    x_antfly_schema_validation_token: str,
) -> Response[Any | Error | SchemaValidationStatus]:
    """Observe an exact validation-only schema publication

     Requires administration of the current logical table. This read never replays DDL; absent or
    unavailable observations cannot prove non-admission.

    Args:
        table_name (str):
        x_antfly_schema_validation_token (str): Opaque immutable validation-only schema job
            identity. Retain it across timeouts or lost replies and observe its exact status without
            replaying DDL. It grants no authorization.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SchemaValidationStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        x_antfly_schema_validation_token=x_antfly_schema_validation_token,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_schema_validation_token: str,
) -> Any | Error | SchemaValidationStatus | None:
    """Observe an exact validation-only schema publication

     Requires administration of the current logical table. This read never replays DDL; absent or
    unavailable observations cannot prove non-admission.

    Args:
        table_name (str):
        x_antfly_schema_validation_token (str): Opaque immutable validation-only schema job
            identity. Retain it across timeouts or lost replies and observe its exact status without
            replaying DDL. It grants no authorization.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SchemaValidationStatus
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        x_antfly_schema_validation_token=x_antfly_schema_validation_token,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_schema_validation_token: str,
) -> Response[Any | Error | SchemaValidationStatus]:
    """Observe an exact validation-only schema publication

     Requires administration of the current logical table. This read never replays DDL; absent or
    unavailable observations cannot prove non-admission.

    Args:
        table_name (str):
        x_antfly_schema_validation_token (str): Opaque immutable validation-only schema job
            identity. Retain it across timeouts or lost replies and observe its exact status without
            replaying DDL. It grants no authorization.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | SchemaValidationStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        x_antfly_schema_validation_token=x_antfly_schema_validation_token,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    x_antfly_schema_validation_token: str,
) -> Any | Error | SchemaValidationStatus | None:
    """Observe an exact validation-only schema publication

     Requires administration of the current logical table. This read never replays DDL; absent or
    unavailable observations cannot prove non-admission.

    Args:
        table_name (str):
        x_antfly_schema_validation_token (str): Opaque immutable validation-only schema job
            identity. Retain it across timeouts or lost replies and observe its exact status without
            replaying DDL. It grants no authorization.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | SchemaValidationStatus
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            x_antfly_schema_validation_token=x_antfly_schema_validation_token,
        )
    ).parsed
