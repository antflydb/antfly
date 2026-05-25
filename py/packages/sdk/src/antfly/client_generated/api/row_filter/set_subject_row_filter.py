from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.row_filter_entry import RowFilterEntry
from ...models.set_subject_row_filter_body import SetSubjectRowFilterBody
from ...types import Response


def _get_kwargs(
    subject: str,
    table: str,
    *,
    body: SetSubjectRowFilterBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "put",
        "url": "/auth/v1/subjects/{subject}/row-filters/{table}".format(
            subject=quote(str(subject), safe=""),
            table=quote(str(table), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | RowFilterEntry | None:
    if response.status_code == 200:
        response_200 = RowFilterEntry.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Error | RowFilterEntry]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
    body: SetSubjectRowFilterBody,
) -> Response[Error | RowFilterEntry]:
    """Set row filter for an auth subject on a table

     Sets or replaces a row filter policy for a role, group, or other Casbin subject.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.
        body (SetSubjectRowFilterBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        subject=subject,
        table=table,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
    body: SetSubjectRowFilterBody,
) -> Error | RowFilterEntry | None:
    """Set row filter for an auth subject on a table

     Sets or replaces a row filter policy for a role, group, or other Casbin subject.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.
        body (SetSubjectRowFilterBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return sync_detailed(
        subject=subject,
        table=table,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
    body: SetSubjectRowFilterBody,
) -> Response[Error | RowFilterEntry]:
    """Set row filter for an auth subject on a table

     Sets or replaces a row filter policy for a role, group, or other Casbin subject.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.
        body (SetSubjectRowFilterBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | RowFilterEntry]
    """

    kwargs = _get_kwargs(
        subject=subject,
        table=table,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    subject: str,
    table: str,
    *,
    client: AuthenticatedClient,
    body: SetSubjectRowFilterBody,
) -> Error | RowFilterEntry | None:
    """Set row filter for an auth subject on a table

     Sets or replaces a row filter policy for a role, group, or other Casbin subject.

    Args:
        subject (str):  Example: role:tenant_reader.
        table (str):  Example: orders.
        body (SetSubjectRowFilterBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | RowFilterEntry
    """

    return (
        await asyncio_detailed(
            subject=subject,
            table=table,
            client=client,
            body=body,
        )
    ).parsed
