from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.privacy_policy import PrivacyPolicy
from ...models.privacy_status import PrivacyStatus
from ...models.put_table_privacy_response_403 import PutTablePrivacyResponse403
from ...models.put_table_privacy_response_412 import PutTablePrivacyResponse412
from ...models.put_table_privacy_response_422 import PutTablePrivacyResponse422
from ...models.put_table_privacy_response_428 import PutTablePrivacyResponse428
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(if_match, Unset):
        headers["If-Match"] = if_match

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/db/v1/tables/{table_name}/privacy".format(
            table_name=quote(str(table_name), safe=""),
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
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
    | None
):
    if response.status_code == 202:
        response_202 = PrivacyStatus.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = PutTablePrivacyResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = PutTablePrivacyResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = PutTablePrivacyResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = PutTablePrivacyResponse428.from_dict(response.json())

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
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
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
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> Response[
    Error
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
]:
    """Apply table privacy policy

    Args:
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyStatus | PutTablePrivacyResponse403 | PutTablePrivacyResponse412 | PutTablePrivacyResponse422 | PutTablePrivacyResponse428]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        if_match=if_match,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> (
    Error
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
    | None
):
    """Apply table privacy policy

    Args:
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyStatus | PutTablePrivacyResponse403 | PutTablePrivacyResponse412 | PutTablePrivacyResponse422 | PutTablePrivacyResponse428
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        body=body,
        if_match=if_match,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> Response[
    Error
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
]:
    """Apply table privacy policy

    Args:
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | PrivacyStatus | PutTablePrivacyResponse403 | PutTablePrivacyResponse412 | PutTablePrivacyResponse422 | PutTablePrivacyResponse428]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        body=body,
        if_match=if_match,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    body: PrivacyPolicy,
    if_match: str | Unset = UNSET,
) -> (
    Error
    | PrivacyStatus
    | PutTablePrivacyResponse403
    | PutTablePrivacyResponse412
    | PutTablePrivacyResponse422
    | PutTablePrivacyResponse428
    | None
):
    """Apply table privacy policy

    Args:
        table_name (str):
        if_match (str | Unset):
        body (PrivacyPolicy):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | PrivacyStatus | PutTablePrivacyResponse403 | PutTablePrivacyResponse412 | PutTablePrivacyResponse422 | PutTablePrivacyResponse428
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
            if_match=if_match,
        )
    ).parsed
