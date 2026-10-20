from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.memory_error import MemoryError_
from ...models.memory_record_page import MemoryRecordPage
from ...types import UNSET, Response, Unset


def _get_kwargs(
    space: str,
    *,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
    subject: str | Unset = UNSET,
    predicate: str | Unset = UNSET,
    scope: str | Unset = UNSET,
    task: str | Unset = UNSET,
    environment: str | Unset = UNSET,
    kind: str | Unset = UNSET,
    relationship: str | Unset = UNSET,
    target_id: str | Unset = UNSET,
    missing_relationship: str | Unset = UNSET,
    include_history: bool | Unset = UNSET,
    include_provisional: bool | Unset = UNSET,
    valid_at: int | Unset = UNSET,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    if not isinstance(x_antfly_database, Unset):
        headers["X-Antfly-Database"] = x_antfly_database

    if not isinstance(x_antfly_namespace, Unset):
        headers["X-Antfly-Namespace"] = x_antfly_namespace

    params: dict[str, Any] = {}

    params["cursor"] = cursor

    params["limit"] = limit

    params["subject"] = subject

    params["predicate"] = predicate

    params["scope"] = scope

    params["task"] = task

    params["environment"] = environment

    params["kind"] = kind

    params["relationship"] = relationship

    params["target_id"] = target_id

    params["missing_relationship"] = missing_relationship

    params["include_history"] = include_history

    params["include_provisional"] = include_provisional

    params["valid_at"] = valid_at

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/memory/v1/spaces/{space}/memories".format(
            space=quote(str(space), safe=""),
        ),
        "params": params,
    }

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> MemoryError_ | MemoryRecordPage | None:
    if response.status_code == 200:
        response_200 = MemoryRecordPage.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = MemoryError_.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = MemoryError_.from_dict(response.json())

        return response_401

    if response.status_code == 403:
        response_403 = MemoryError_.from_dict(response.json())

        return response_403

    if response.status_code == 404:
        response_404 = MemoryError_.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = MemoryError_.from_dict(response.json())

        return response_409

    if response.status_code == 413:
        response_413 = MemoryError_.from_dict(response.json())

        return response_413

    if response.status_code == 422:
        response_422 = MemoryError_.from_dict(response.json())

        return response_422

    if response.status_code == 429:
        response_429 = MemoryError_.from_dict(response.json())

        return response_429

    if response.status_code == 503:
        response_503 = MemoryError_.from_dict(response.json())

        return response_503

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[MemoryError_ | MemoryRecordPage]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    space: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
    subject: str | Unset = UNSET,
    predicate: str | Unset = UNSET,
    scope: str | Unset = UNSET,
    task: str | Unset = UNSET,
    environment: str | Unset = UNSET,
    kind: str | Unset = UNSET,
    relationship: str | Unset = UNSET,
    target_id: str | Unset = UNSET,
    missing_relationship: str | Unset = UNSET,
    include_history: bool | Unset = UNSET,
    include_provisional: bool | Unset = UNSET,
    valid_at: int | Unset = UNSET,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
) -> Response[MemoryError_ | MemoryRecordPage]:
    """
    Args:
        space (str):
        cursor (str | Unset):
        limit (int | Unset):
        subject (str | Unset):
        predicate (str | Unset):
        scope (str | Unset):
        task (str | Unset):
        environment (str | Unset):
        kind (str | Unset):
        relationship (str | Unset):
        target_id (str | Unset):
        missing_relationship (str | Unset):
        include_history (bool | Unset):
        include_provisional (bool | Unset):
        valid_at (int | Unset):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MemoryError_ | MemoryRecordPage]
    """

    kwargs = _get_kwargs(
        space=space,
        cursor=cursor,
        limit=limit,
        subject=subject,
        predicate=predicate,
        scope=scope,
        task=task,
        environment=environment,
        kind=kind,
        relationship=relationship,
        target_id=target_id,
        missing_relationship=missing_relationship,
        include_history=include_history,
        include_provisional=include_provisional,
        valid_at=valid_at,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    space: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
    subject: str | Unset = UNSET,
    predicate: str | Unset = UNSET,
    scope: str | Unset = UNSET,
    task: str | Unset = UNSET,
    environment: str | Unset = UNSET,
    kind: str | Unset = UNSET,
    relationship: str | Unset = UNSET,
    target_id: str | Unset = UNSET,
    missing_relationship: str | Unset = UNSET,
    include_history: bool | Unset = UNSET,
    include_provisional: bool | Unset = UNSET,
    valid_at: int | Unset = UNSET,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
) -> MemoryError_ | MemoryRecordPage | None:
    """
    Args:
        space (str):
        cursor (str | Unset):
        limit (int | Unset):
        subject (str | Unset):
        predicate (str | Unset):
        scope (str | Unset):
        task (str | Unset):
        environment (str | Unset):
        kind (str | Unset):
        relationship (str | Unset):
        target_id (str | Unset):
        missing_relationship (str | Unset):
        include_history (bool | Unset):
        include_provisional (bool | Unset):
        valid_at (int | Unset):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MemoryError_ | MemoryRecordPage
    """

    return sync_detailed(
        space=space,
        client=client,
        cursor=cursor,
        limit=limit,
        subject=subject,
        predicate=predicate,
        scope=scope,
        task=task,
        environment=environment,
        kind=kind,
        relationship=relationship,
        target_id=target_id,
        missing_relationship=missing_relationship,
        include_history=include_history,
        include_provisional=include_provisional,
        valid_at=valid_at,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
    ).parsed


async def asyncio_detailed(
    space: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
    subject: str | Unset = UNSET,
    predicate: str | Unset = UNSET,
    scope: str | Unset = UNSET,
    task: str | Unset = UNSET,
    environment: str | Unset = UNSET,
    kind: str | Unset = UNSET,
    relationship: str | Unset = UNSET,
    target_id: str | Unset = UNSET,
    missing_relationship: str | Unset = UNSET,
    include_history: bool | Unset = UNSET,
    include_provisional: bool | Unset = UNSET,
    valid_at: int | Unset = UNSET,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
) -> Response[MemoryError_ | MemoryRecordPage]:
    """
    Args:
        space (str):
        cursor (str | Unset):
        limit (int | Unset):
        subject (str | Unset):
        predicate (str | Unset):
        scope (str | Unset):
        task (str | Unset):
        environment (str | Unset):
        kind (str | Unset):
        relationship (str | Unset):
        target_id (str | Unset):
        missing_relationship (str | Unset):
        include_history (bool | Unset):
        include_provisional (bool | Unset):
        valid_at (int | Unset):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[MemoryError_ | MemoryRecordPage]
    """

    kwargs = _get_kwargs(
        space=space,
        cursor=cursor,
        limit=limit,
        subject=subject,
        predicate=predicate,
        scope=scope,
        task=task,
        environment=environment,
        kind=kind,
        relationship=relationship,
        target_id=target_id,
        missing_relationship=missing_relationship,
        include_history=include_history,
        include_provisional=include_provisional,
        valid_at=valid_at,
        x_antfly_database=x_antfly_database,
        x_antfly_namespace=x_antfly_namespace,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    space: str,
    *,
    client: AuthenticatedClient | Client,
    cursor: str | Unset = UNSET,
    limit: int | Unset = UNSET,
    subject: str | Unset = UNSET,
    predicate: str | Unset = UNSET,
    scope: str | Unset = UNSET,
    task: str | Unset = UNSET,
    environment: str | Unset = UNSET,
    kind: str | Unset = UNSET,
    relationship: str | Unset = UNSET,
    target_id: str | Unset = UNSET,
    missing_relationship: str | Unset = UNSET,
    include_history: bool | Unset = UNSET,
    include_provisional: bool | Unset = UNSET,
    valid_at: int | Unset = UNSET,
    x_antfly_database: str | Unset = "default",
    x_antfly_namespace: str | Unset = "public",
) -> MemoryError_ | MemoryRecordPage | None:
    """
    Args:
        space (str):
        cursor (str | Unset):
        limit (int | Unset):
        subject (str | Unset):
        predicate (str | Unset):
        scope (str | Unset):
        task (str | Unset):
        environment (str | Unset):
        kind (str | Unset):
        relationship (str | Unset):
        target_id (str | Unset):
        missing_relationship (str | Unset):
        include_history (bool | Unset):
        include_provisional (bool | Unset):
        valid_at (int | Unset):
        x_antfly_database (str | Unset):  Default: 'default'.
        x_antfly_namespace (str | Unset):  Default: 'public'.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        MemoryError_ | MemoryRecordPage
    """

    return (
        await asyncio_detailed(
            space=space,
            client=client,
            cursor=cursor,
            limit=limit,
            subject=subject,
            predicate=predicate,
            scope=scope,
            task=task,
            environment=environment,
            kind=kind,
            relationship=relationship,
            target_id=target_id,
            missing_relationship=missing_relationship,
            include_history=include_history,
            include_provisional=include_provisional,
            valid_at=valid_at,
            x_antfly_database=x_antfly_database,
            x_antfly_namespace=x_antfly_namespace,
        )
    ).parsed
