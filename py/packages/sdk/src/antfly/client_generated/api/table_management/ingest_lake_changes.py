from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.ingest_lake_changes_body import IngestLakeChangesBody
from ...models.ingest_lake_changes_response_202 import IngestLakeChangesResponse202
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: IngestLakeChangesBody,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/lake/changes".format(
            table_name=quote(str(table_name), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | IngestLakeChangesResponse202 | None:
    if response.status_code == 202:
        response_202 = IngestLakeChangesResponse202.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = cast(Any, None)
        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

    if response.status_code == 404:
        response_404 = cast(Any, None)
        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 503:
        response_503 = cast(Any, None)
        return response_503

    if response.status_code == 504:
        response_504 = cast(Any, None)
        return response_504

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | IngestLakeChangesResponse202]:
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
    body: IngestLakeChangesBody,
) -> Response[Any | IngestLakeChangesResponse202]:
    """ingestLakeChanges

     Durably accept one complete CDC transaction for native WAL-to-Iceberg writing. A stable batch ID,
    source epoch, key fields and predecessor checkpoint are required. Upserts are complete row images;
    deletes contain only key fields. Acceptance precedes catalog commitment and searchable index
    publication. Requires table admin permission and iceberg_writer policy.

    Args:
        table_name (str):
        body (IngestLakeChangesBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | IngestLakeChangesResponse202]
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
    body: IngestLakeChangesBody,
) -> Any | IngestLakeChangesResponse202 | None:
    """ingestLakeChanges

     Durably accept one complete CDC transaction for native WAL-to-Iceberg writing. A stable batch ID,
    source epoch, key fields and predecessor checkpoint are required. Upserts are complete row images;
    deletes contain only key fields. Acceptance precedes catalog commitment and searchable index
    publication. Requires table admin permission and iceberg_writer policy.

    Args:
        table_name (str):
        body (IngestLakeChangesBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | IngestLakeChangesResponse202
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
    body: IngestLakeChangesBody,
) -> Response[Any | IngestLakeChangesResponse202]:
    """ingestLakeChanges

     Durably accept one complete CDC transaction for native WAL-to-Iceberg writing. A stable batch ID,
    source epoch, key fields and predecessor checkpoint are required. Upserts are complete row images;
    deletes contain only key fields. Acceptance precedes catalog commitment and searchable index
    publication. Requires table admin permission and iceberg_writer policy.

    Args:
        table_name (str):
        body (IngestLakeChangesBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | IngestLakeChangesResponse202]
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
    body: IngestLakeChangesBody,
) -> Any | IngestLakeChangesResponse202 | None:
    """ingestLakeChanges

     Durably accept one complete CDC transaction for native WAL-to-Iceberg writing. A stable batch ID,
    source epoch, key fields and predecessor checkpoint are required. Upserts are complete row images;
    deletes contain only key fields. Acceptance precedes catalog commitment and searchable index
    publication. Requires table admin permission and iceberg_writer policy.

    Args:
        table_name (str):
        body (IngestLakeChangesBody):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | IngestLakeChangesResponse202
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
