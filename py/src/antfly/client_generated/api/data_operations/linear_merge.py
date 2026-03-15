from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.linear_merge_request import LinearMergeRequest
from ...models.linear_merge_result import LinearMergeResult
from ...types import Response


def _get_kwargs(
    table_name: str,
    *,
    body: LinearMergeRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/tables/{table_name}/merge",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, LinearMergeResult]]:
    if response.status_code == 200:
        response_200 = LinearMergeResult.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, LinearMergeResult]]:
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
    body: LinearMergeRequest,
) -> Response[Union[Error, LinearMergeResult]]:
    """Synchronize data from external sources (Shopify, Postgres, S3) using a linear merge

     Synchronize and keep Antfly in sync with external data sources like Shopify,
    Postgres, S3, or any sorted record source. Also known as: data synchronization,
    database sync, incremental sync, e-commerce sync.

    Both source and destination must be sorted by the same key. Performs three-way merge:
    - Inserts new records from source
    - Updates changed records
    - Deletes Antfly records absent from source page

    **Stateless & Idempotent**: No sync state between pages. Safe to restart
    from any page if interrupted.

    **Use Cases**: Sync production databases, e-commerce APIs (Shopify, WooCommerce),
    data lake exports, or warehouse tables to Antfly for low-latency hybrid search.

    **WARNING**: Not safe for concurrent merges with overlapping ranges.
    Single-client sync API only.

    Args:
        table_name (str):
        body (LinearMergeRequest): Linear merge operation for syncing sorted records from external
            sources.
            Use this to keep Antfly in sync with an external database or data source.

            **How it works:**
            1. Send sorted records from your external source
            2. Server upserts records that exist in your batch
            3. Server deletes Antfly records in the key range that are absent from your batch
            4. If stopped at shard boundary, use next_cursor for next request

            **WARNING:** Not safe for concurrent operations with overlapping key ranges.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, LinearMergeResult]]
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
    body: LinearMergeRequest,
) -> Optional[Union[Error, LinearMergeResult]]:
    """Synchronize data from external sources (Shopify, Postgres, S3) using a linear merge

     Synchronize and keep Antfly in sync with external data sources like Shopify,
    Postgres, S3, or any sorted record source. Also known as: data synchronization,
    database sync, incremental sync, e-commerce sync.

    Both source and destination must be sorted by the same key. Performs three-way merge:
    - Inserts new records from source
    - Updates changed records
    - Deletes Antfly records absent from source page

    **Stateless & Idempotent**: No sync state between pages. Safe to restart
    from any page if interrupted.

    **Use Cases**: Sync production databases, e-commerce APIs (Shopify, WooCommerce),
    data lake exports, or warehouse tables to Antfly for low-latency hybrid search.

    **WARNING**: Not safe for concurrent merges with overlapping ranges.
    Single-client sync API only.

    Args:
        table_name (str):
        body (LinearMergeRequest): Linear merge operation for syncing sorted records from external
            sources.
            Use this to keep Antfly in sync with an external database or data source.

            **How it works:**
            1. Send sorted records from your external source
            2. Server upserts records that exist in your batch
            3. Server deletes Antfly records in the key range that are absent from your batch
            4. If stopped at shard boundary, use next_cursor for next request

            **WARNING:** Not safe for concurrent operations with overlapping key ranges.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, LinearMergeResult]
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
    body: LinearMergeRequest,
) -> Response[Union[Error, LinearMergeResult]]:
    """Synchronize data from external sources (Shopify, Postgres, S3) using a linear merge

     Synchronize and keep Antfly in sync with external data sources like Shopify,
    Postgres, S3, or any sorted record source. Also known as: data synchronization,
    database sync, incremental sync, e-commerce sync.

    Both source and destination must be sorted by the same key. Performs three-way merge:
    - Inserts new records from source
    - Updates changed records
    - Deletes Antfly records absent from source page

    **Stateless & Idempotent**: No sync state between pages. Safe to restart
    from any page if interrupted.

    **Use Cases**: Sync production databases, e-commerce APIs (Shopify, WooCommerce),
    data lake exports, or warehouse tables to Antfly for low-latency hybrid search.

    **WARNING**: Not safe for concurrent merges with overlapping ranges.
    Single-client sync API only.

    Args:
        table_name (str):
        body (LinearMergeRequest): Linear merge operation for syncing sorted records from external
            sources.
            Use this to keep Antfly in sync with an external database or data source.

            **How it works:**
            1. Send sorted records from your external source
            2. Server upserts records that exist in your batch
            3. Server deletes Antfly records in the key range that are absent from your batch
            4. If stopped at shard boundary, use next_cursor for next request

            **WARNING:** Not safe for concurrent operations with overlapping key ranges.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, LinearMergeResult]]
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
    body: LinearMergeRequest,
) -> Optional[Union[Error, LinearMergeResult]]:
    """Synchronize data from external sources (Shopify, Postgres, S3) using a linear merge

     Synchronize and keep Antfly in sync with external data sources like Shopify,
    Postgres, S3, or any sorted record source. Also known as: data synchronization,
    database sync, incremental sync, e-commerce sync.

    Both source and destination must be sorted by the same key. Performs three-way merge:
    - Inserts new records from source
    - Updates changed records
    - Deletes Antfly records absent from source page

    **Stateless & Idempotent**: No sync state between pages. Safe to restart
    from any page if interrupted.

    **Use Cases**: Sync production databases, e-commerce APIs (Shopify, WooCommerce),
    data lake exports, or warehouse tables to Antfly for low-latency hybrid search.

    **WARNING**: Not safe for concurrent merges with overlapping ranges.
    Single-client sync API only.

    Args:
        table_name (str):
        body (LinearMergeRequest): Linear merge operation for syncing sorted records from external
            sources.
            Use this to keep Antfly in sync with an external database or data source.

            **How it works:**
            1. Send sorted records from your external source
            2. Server upserts records that exist in your batch
            3. Server deletes Antfly records in the key range that are absent from your batch
            4. If stopped at shard boundary, use next_cursor for next request

            **WARNING:** Not safe for concurrent operations with overlapping key ranges.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, LinearMergeResult]
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            client=client,
            body=body,
        )
    ).parsed
