from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.list_table_privacy_findings_response_403 import ListTablePrivacyFindingsResponse403
from ...models.list_table_privacy_findings_response_412 import ListTablePrivacyFindingsResponse412
from ...models.list_table_privacy_findings_response_422 import ListTablePrivacyFindingsResponse422
from ...models.list_table_privacy_findings_response_428 import ListTablePrivacyFindingsResponse428
from ...models.privacy_findings_page import PrivacyFindingsPage
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    *,
    cursor: str | Unset = UNSET,
    document: str | Unset = UNSET,
    field: str | Unset = UNSET,
    category: str | Unset = UNSET,
    policy_version: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["document"] = document

    params["field"] = field

    params["category"] = category

    params["policy_version"] = policy_version

    params["limit"] = limit

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/privacy/findings".format(
            table_name=quote(str(table_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
    | None
):
    if response.status_code == 200:
        response_200 = PrivacyFindingsPage.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = ListTablePrivacyFindingsResponse403.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 412:
        response_412 = ListTablePrivacyFindingsResponse412.from_dict(response.json())

        return response_412

    if response.status_code == 422:
        response_422 = ListTablePrivacyFindingsResponse422.from_dict(response.json())

        return response_422

    if response.status_code == 428:
        response_428 = ListTablePrivacyFindingsResponse428.from_dict(response.json())

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
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
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
    cursor: str | Unset = UNSET,
    document: str | Unset = UNSET,
    field: str | Unset = UNSET,
    category: str | Unset = UNSET,
    policy_version: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> Response[
    Error
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
]:
    """List current privacy findings

    Args:
        table_name (str):
        cursor (str | Unset):
        document (str | Unset):
        field (str | Unset):
        category (str | Unset):
        policy_version (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ListTablePrivacyFindingsResponse403 | ListTablePrivacyFindingsResponse412 | ListTablePrivacyFindingsResponse422 | ListTablePrivacyFindingsResponse428 | PrivacyFindingsPage]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        cursor=cursor,
        document=document,
        field=field,
        category=category,
        policy_version=policy_version,
        limit=limit,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    document: str | Unset = UNSET,
    field: str | Unset = UNSET,
    category: str | Unset = UNSET,
    policy_version: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> (
    Error
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
    | None
):
    """List current privacy findings

    Args:
        table_name (str):
        cursor (str | Unset):
        document (str | Unset):
        field (str | Unset):
        category (str | Unset):
        policy_version (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ListTablePrivacyFindingsResponse403 | ListTablePrivacyFindingsResponse412 | ListTablePrivacyFindingsResponse422 | ListTablePrivacyFindingsResponse428 | PrivacyFindingsPage
    """

    return sync_detailed(
        table_name=table_name,
        client=client,
        cursor=cursor,
        document=document,
        field=field,
        category=category,
        policy_version=policy_version,
        limit=limit,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    document: str | Unset = UNSET,
    field: str | Unset = UNSET,
    category: str | Unset = UNSET,
    policy_version: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> Response[
    Error
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
]:
    """List current privacy findings

    Args:
        table_name (str):
        cursor (str | Unset):
        document (str | Unset):
        field (str | Unset):
        category (str | Unset):
        policy_version (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ListTablePrivacyFindingsResponse403 | ListTablePrivacyFindingsResponse412 | ListTablePrivacyFindingsResponse422 | ListTablePrivacyFindingsResponse428 | PrivacyFindingsPage]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        cursor=cursor,
        document=document,
        field=field,
        category=category,
        policy_version=policy_version,
        limit=limit,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    *,
    client: AuthenticatedClient,
    cursor: str | Unset = UNSET,
    document: str | Unset = UNSET,
    field: str | Unset = UNSET,
    category: str | Unset = UNSET,
    policy_version: str | Unset = UNSET,
    limit: int | Unset = UNSET,
) -> (
    Error
    | ListTablePrivacyFindingsResponse403
    | ListTablePrivacyFindingsResponse412
    | ListTablePrivacyFindingsResponse422
    | ListTablePrivacyFindingsResponse428
    | PrivacyFindingsPage
    | None
):
    """List current privacy findings

    Args:
        table_name (str):
        cursor (str | Unset):
        document (str | Unset):
        field (str | Unset):
        category (str | Unset):
        policy_version (str | Unset):
        limit (int | Unset):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ListTablePrivacyFindingsResponse403 | ListTablePrivacyFindingsResponse412 | ListTablePrivacyFindingsResponse422 | ListTablePrivacyFindingsResponse428 | PrivacyFindingsPage
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            cursor=cursor,
            document=document,
            field=field,
            category=category,
            policy_version=policy_version,
            limit=limit,
        )
    ).parsed
