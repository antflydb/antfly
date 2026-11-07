from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.get_table_privacy_response_403 import GetTablePrivacyResponse403
from ...models.get_table_privacy_response_412 import GetTablePrivacyResponse412
from ...models.get_table_privacy_response_422 import GetTablePrivacyResponse422
from ...models.get_table_privacy_response_428 import GetTablePrivacyResponse428
from ...models.privacy_status import PrivacyStatus
from ...types import Response


def _get_kwargs(
    table_name: str,
) -> dict[str, Any]:

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/privacy".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
    | None
):
    if response.status_code == 200:
        response_200 = PrivacyStatus.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = GetTablePrivacyResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = GetTablePrivacyResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = GetTablePrivacyResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = GetTablePrivacyResponse428.from_dict(response.json())

        return response_428

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = Error.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
]:
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
) -> Response[
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
]:
    """Read table privacy policy and coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetTablePrivacyResponse403 | GetTablePrivacyResponse412 | GetTablePrivacyResponse422 | GetTablePrivacyResponse428 | PrivacyStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> (
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
    | None
):
    """Read table privacy policy and coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetTablePrivacyResponse403 | GetTablePrivacyResponse412 | GetTablePrivacyResponse422 | GetTablePrivacyResponse428 | PrivacyStatus
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> Response[
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
]:
    """Read table privacy policy and coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | GetTablePrivacyResponse403 | GetTablePrivacyResponse412 | GetTablePrivacyResponse422 | GetTablePrivacyResponse428 | PrivacyStatus]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
) -> (
    Error
    | GetTablePrivacyResponse403
    | GetTablePrivacyResponse412
    | GetTablePrivacyResponse422
    | GetTablePrivacyResponse428
    | PrivacyStatus
    | None
):
    """Read table privacy policy and coverage

    Args:
        table_name (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | GetTablePrivacyResponse403 | GetTablePrivacyResponse412 | GetTablePrivacyResponse422 | GetTablePrivacyResponse428 | PrivacyStatus
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
        )
    ).parsed
