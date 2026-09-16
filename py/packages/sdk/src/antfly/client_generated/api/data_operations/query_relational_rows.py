from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.relational_row_query_request import RelationalRowQueryRequest
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: RelationalRowQueryRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/rows/query".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Any | Error | str | None:
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Any | Error | str]:
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
    body: RelationalRowQueryRequest,
) -> Response[Any | Error | str]:
    """Query projected typed relational rows

    Args:
        table_name (str):
        body (RelationalRowQueryRequest): Bounded relational scan in primary-key order, or
            composite index order
            when index is supplied. Index queries require schema_version and every
            owning shard must have the selected generation ready. Partial indexes
            require their WHERE predicates to be implied by the query conditions.
            The bounded proof combines per-column equality, tighter ranges,
            exclusions, and NULL-aware predicates using exact typed values and
            matching collations. Unsupported implications fail closed. Explicit
            scan bounds alone are not an implication proof. Equal tuples are
            ordered by primary key. Each shard read pins its own immutable schema
            and row snapshot; this is not a table-wide consistent snapshot.
            Resume with the last returned _id as from for primary scans, or its
            cursor as after for index scans. A resumed request opens a fresh snapshot,
            not a retained cursor; concurrent mutations may move rows across the
            continuation boundary. Keep index, bounds and conditions unchanged when paging.
            An empty projection returns row identities and versions only.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | str]
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
    body: RelationalRowQueryRequest,
) -> Any | Error | str | None:
    """Query projected typed relational rows

    Args:
        table_name (str):
        body (RelationalRowQueryRequest): Bounded relational scan in primary-key order, or
            composite index order
            when index is supplied. Index queries require schema_version and every
            owning shard must have the selected generation ready. Partial indexes
            require their WHERE predicates to be implied by the query conditions.
            The bounded proof combines per-column equality, tighter ranges,
            exclusions, and NULL-aware predicates using exact typed values and
            matching collations. Unsupported implications fail closed. Explicit
            scan bounds alone are not an implication proof. Equal tuples are
            ordered by primary key. Each shard read pins its own immutable schema
            and row snapshot; this is not a table-wide consistent snapshot.
            Resume with the last returned _id as from for primary scans, or its
            cursor as after for index scans. A resumed request opens a fresh snapshot,
            not a retained cursor; concurrent mutations may move rows across the
            continuation boundary. Keep index, bounds and conditions unchanged when paging.
            An empty projection returns row identities and versions only.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | str
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
    body: RelationalRowQueryRequest,
) -> Response[Any | Error | str]:
    """Query projected typed relational rows

    Args:
        table_name (str):
        body (RelationalRowQueryRequest): Bounded relational scan in primary-key order, or
            composite index order
            when index is supplied. Index queries require schema_version and every
            owning shard must have the selected generation ready. Partial indexes
            require their WHERE predicates to be implied by the query conditions.
            The bounded proof combines per-column equality, tighter ranges,
            exclusions, and NULL-aware predicates using exact typed values and
            matching collations. Unsupported implications fail closed. Explicit
            scan bounds alone are not an implication proof. Equal tuples are
            ordered by primary key. Each shard read pins its own immutable schema
            and row snapshot; this is not a table-wide consistent snapshot.
            Resume with the last returned _id as from for primary scans, or its
            cursor as after for index scans. A resumed request opens a fresh snapshot,
            not a retained cursor; concurrent mutations may move rows across the
            continuation boundary. Keep index, bounds and conditions unchanged when paging.
            An empty projection returns row identities and versions only.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | str]
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
    body: RelationalRowQueryRequest,
) -> Any | Error | str | None:
    """Query projected typed relational rows

    Args:
        table_name (str):
        body (RelationalRowQueryRequest): Bounded relational scan in primary-key order, or
            composite index order
            when index is supplied. Index queries require schema_version and every
            owning shard must have the selected generation ready. Partial indexes
            require their WHERE predicates to be implied by the query conditions.
            The bounded proof combines per-column equality, tighter ranges,
            exclusions, and NULL-aware predicates using exact typed values and
            matching collations. Unsupported implications fail closed. Explicit
            scan bounds alone are not an implication proof. Equal tuples are
            ordered by primary key. Each shard read pins its own immutable schema
            and row snapshot; this is not a table-wide consistent snapshot.
            Resume with the last returned _id as from for primary scans, or its
            cursor as after for index scans. A resumed request opens a fresh snapshot,
            not a retained cursor; concurrent mutations may move rows across the
            continuation boundary. Keep index, bounds and conditions unchanged when paging.
            An empty projection returns row identities and versions only.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | str
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
