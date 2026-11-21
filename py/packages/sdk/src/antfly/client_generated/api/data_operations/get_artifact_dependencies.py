from http import HTTPStatus
from typing import Any
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.artifact_dependencies import ArtifactDependencies
from ...models.artifact_not_current import ArtifactNotCurrent
from ...models.error import Error
from ...models.get_artifact_dependencies_freshness import GetArtifactDependenciesFreshness
from ...types import UNSET, Response, Unset


def _get_kwargs(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    freshness: GetArtifactDependenciesFreshness | Unset = GetArtifactDependenciesFreshness.ANY,
    wait_timeout_ms: int | Unset = 0,
) -> dict[str, Any]:

    params: dict[str, Any] = {}

    json_freshness: str | Unset = UNSET
    if not isinstance(freshness, Unset):
        json_freshness = freshness.value

    params["freshness"] = json_freshness

    params["wait_timeout_ms"] = wait_timeout_ms

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/db/v1/tables/{table_name}/documents/{key}/artifacts/{artifact_name}/dependencies".format(
            table_name=quote(str(table_name), safe=""),
            key=quote(str(key), safe=""),
            artifact_name=quote(str(artifact_name), safe=""),
        ),
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> ArtifactDependencies | ArtifactNotCurrent | Error | None:
    if response.status_code == 200:
        response_200 = ArtifactDependencies.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = ArtifactNotCurrent.from_dict(response.json())

        return response_409

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
) -> Response[ArtifactDependencies | ArtifactNotCurrent | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    freshness: GetArtifactDependenciesFreshness | Unset = GetArtifactDependenciesFreshness.ANY,
    wait_timeout_ms: int | Unset = 0,
) -> Response[ArtifactDependencies | ArtifactNotCurrent | Error]:
    """Inspect an artifact's context dependencies and freshness

     Returns the dependency state of one generated artifact: its freshness,
    live subscriptions, and retained lineage manifests. With
    `freshness=require_current` the read waits up to `wait_timeout_ms` for
    a read barrier: every group of every source table the enrichment reads
    has delivered all committed change notices for this table, this
    group's inbox has applied them, and the artifact's state is current.
    Otherwise it answers 409 `artifact_not_current`. The route is
    shard-aware and requires read permission on the table.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        freshness (GetArtifactDependenciesFreshness | Unset):  Default:
            GetArtifactDependenciesFreshness.ANY.
        wait_timeout_ms (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactDependencies | ArtifactNotCurrent | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        freshness=freshness,
        wait_timeout_ms=wait_timeout_ms,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    freshness: GetArtifactDependenciesFreshness | Unset = GetArtifactDependenciesFreshness.ANY,
    wait_timeout_ms: int | Unset = 0,
) -> ArtifactDependencies | ArtifactNotCurrent | Error | None:
    """Inspect an artifact's context dependencies and freshness

     Returns the dependency state of one generated artifact: its freshness,
    live subscriptions, and retained lineage manifests. With
    `freshness=require_current` the read waits up to `wait_timeout_ms` for
    a read barrier: every group of every source table the enrichment reads
    has delivered all committed change notices for this table, this
    group's inbox has applied them, and the artifact's state is current.
    Otherwise it answers 409 `artifact_not_current`. The route is
    shard-aware and requires read permission on the table.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        freshness (GetArtifactDependenciesFreshness | Unset):  Default:
            GetArtifactDependenciesFreshness.ANY.
        wait_timeout_ms (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactDependencies | ArtifactNotCurrent | Error
    """

    return sync_detailed(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        client=client,
        freshness=freshness,
        wait_timeout_ms=wait_timeout_ms,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    freshness: GetArtifactDependenciesFreshness | Unset = GetArtifactDependenciesFreshness.ANY,
    wait_timeout_ms: int | Unset = 0,
) -> Response[ArtifactDependencies | ArtifactNotCurrent | Error]:
    """Inspect an artifact's context dependencies and freshness

     Returns the dependency state of one generated artifact: its freshness,
    live subscriptions, and retained lineage manifests. With
    `freshness=require_current` the read waits up to `wait_timeout_ms` for
    a read barrier: every group of every source table the enrichment reads
    has delivered all committed change notices for this table, this
    group's inbox has applied them, and the artifact's state is current.
    Otherwise it answers 409 `artifact_not_current`. The route is
    shard-aware and requires read permission on the table.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        freshness (GetArtifactDependenciesFreshness | Unset):  Default:
            GetArtifactDependenciesFreshness.ANY.
        wait_timeout_ms (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[ArtifactDependencies | ArtifactNotCurrent | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        key=key,
        artifact_name=artifact_name,
        freshness=freshness,
        wait_timeout_ms=wait_timeout_ms,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    key: str,
    artifact_name: str,
    *,
    client: AuthenticatedClient,
    freshness: GetArtifactDependenciesFreshness | Unset = GetArtifactDependenciesFreshness.ANY,
    wait_timeout_ms: int | Unset = 0,
) -> ArtifactDependencies | ArtifactNotCurrent | Error | None:
    """Inspect an artifact's context dependencies and freshness

     Returns the dependency state of one generated artifact: its freshness,
    live subscriptions, and retained lineage manifests. With
    `freshness=require_current` the read waits up to `wait_timeout_ms` for
    a read barrier: every group of every source table the enrichment reads
    has delivered all committed change notices for this table, this
    group's inbox has applied them, and the artifact's state is current.
    Otherwise it answers 409 `artifact_not_current`. The route is
    shard-aware and requires read permission on the table.

    Args:
        table_name (str):
        key (str):
        artifact_name (str):
        freshness (GetArtifactDependenciesFreshness | Unset):  Default:
            GetArtifactDependenciesFreshness.ANY.
        wait_timeout_ms (int | Unset):  Default: 0.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        ArtifactDependencies | ArtifactNotCurrent | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            key=key,
            artifact_name=artifact_name,
            client=client,
            freshness=freshness,
            wait_timeout_ms=wait_timeout_ms,
        )
    ).parsed
