from http import HTTPStatus
from typing import Any, cast
from urllib.parse import quote

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.curation_proposal_decision_request import CurationProposalDecisionRequest
from ...models.curation_proposal_decision_result import CurationProposalDecisionResult
from ...models.error import Error
from ...types import Response


def _get_kwargs(
    table_name: str,
    proposal_id: str,
    *,
    body: CurationProposalDecisionRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/tables/{table_name}/curation/proposals/{proposal_id}:accept".format(
            table_name=quote(str(table_name), safe=""),
            proposal_id=quote(str(proposal_id), safe=""),
        ),
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Any | CurationProposalDecisionResult | Error | None:
    if response.status_code == 200:
        response_200 = CurationProposalDecisionResult.from_dict(response.json())

        return response_200

    if response.status_code == 202:
        response_202 = CurationProposalDecisionResult.from_dict(response.json())

        return response_202

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 409:
        response_409 = cast(Any, None)
        return response_409

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[Any | CurationProposalDecisionResult | Error]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    table_name: str,
    proposal_id: str,
    *,
    client: AuthenticatedClient,
    body: CurationProposalDecisionRequest,
) -> Response[Any | CurationProposalDecisionResult | Error]:
    """acceptCurationProposal

    Args:
        table_name (str):
        proposal_id (str):
        body (CurationProposalDecisionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CurationProposalDecisionResult | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        proposal_id=proposal_id,
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    table_name: str,
    proposal_id: str,
    *,
    client: AuthenticatedClient,
    body: CurationProposalDecisionRequest,
) -> Any | CurationProposalDecisionResult | Error | None:
    """acceptCurationProposal

    Args:
        table_name (str):
        proposal_id (str):
        body (CurationProposalDecisionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CurationProposalDecisionResult | Error
    """

    return sync_detailed(
        table_name=table_name,
        proposal_id=proposal_id,
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    table_name: str,
    proposal_id: str,
    *,
    client: AuthenticatedClient,
    body: CurationProposalDecisionRequest,
) -> Response[Any | CurationProposalDecisionResult | Error]:
    """acceptCurationProposal

    Args:
        table_name (str):
        proposal_id (str):
        body (CurationProposalDecisionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | CurationProposalDecisionResult | Error]
    """

    kwargs = _get_kwargs(
        table_name=table_name,
        proposal_id=proposal_id,
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    table_name: str,
    proposal_id: str,
    *,
    client: AuthenticatedClient,
    body: CurationProposalDecisionRequest,
) -> Any | CurationProposalDecisionResult | Error | None:
    """acceptCurationProposal

    Args:
        table_name (str):
        proposal_id (str):
        body (CurationProposalDecisionRequest):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | CurationProposalDecisionResult | Error
    """

    return (
        await asyncio_detailed(
            table_name=table_name,
            proposal_id=proposal_id,
            client=client,
            body=body,
        )
    ).parsed
