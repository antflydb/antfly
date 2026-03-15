from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.eval_request import EvalRequest
from ...models.eval_result import EvalResult
from ...types import Response


def _get_kwargs(
    *,
    body: EvalRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/eval",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[Error, EvalResult]]:
    if response.status_code == 200:
        response_200 = EvalResult.from_dict(response.json())

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
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[Error, EvalResult]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: EvalRequest,
) -> Response[Union[Error, EvalResult]]:
    """Standalone evaluation endpoint

     Run evaluators on provided data without executing a query.
    Useful for testing evaluators, evaluating cached results, or batch evaluation.

    **Retrieval metrics** (require ground_truth.relevant_ids and retrieved_ids):
    - recall, precision, ndcg, mrr, map

    **LLM-as-judge metrics** (require judge config):
    - relevance, faithfulness, completeness, coherence, safety, helpfulness, correctness,
    citation_quality

    Args:
        body (EvalRequest): Standalone evaluation request for POST /eval endpoint.
            Useful for testing evaluators without running a query.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, EvalResult]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: AuthenticatedClient,
    body: EvalRequest,
) -> Optional[Union[Error, EvalResult]]:
    """Standalone evaluation endpoint

     Run evaluators on provided data without executing a query.
    Useful for testing evaluators, evaluating cached results, or batch evaluation.

    **Retrieval metrics** (require ground_truth.relevant_ids and retrieved_ids):
    - recall, precision, ndcg, mrr, map

    **LLM-as-judge metrics** (require judge config):
    - relevance, faithfulness, completeness, coherence, safety, helpfulness, correctness,
    citation_quality

    Args:
        body (EvalRequest): Standalone evaluation request for POST /eval endpoint.
            Useful for testing evaluators without running a query.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, EvalResult]
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: EvalRequest,
) -> Response[Union[Error, EvalResult]]:
    """Standalone evaluation endpoint

     Run evaluators on provided data without executing a query.
    Useful for testing evaluators, evaluating cached results, or batch evaluation.

    **Retrieval metrics** (require ground_truth.relevant_ids and retrieved_ids):
    - recall, precision, ndcg, mrr, map

    **LLM-as-judge metrics** (require judge config):
    - relevance, faithfulness, completeness, coherence, safety, helpfulness, correctness,
    citation_quality

    Args:
        body (EvalRequest): Standalone evaluation request for POST /eval endpoint.
            Useful for testing evaluators without running a query.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[Error, EvalResult]]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: EvalRequest,
) -> Optional[Union[Error, EvalResult]]:
    """Standalone evaluation endpoint

     Run evaluators on provided data without executing a query.
    Useful for testing evaluators, evaluating cached results, or batch evaluation.

    **Retrieval metrics** (require ground_truth.relevant_ids and retrieved_ids):
    - recall, precision, ndcg, mrr, map

    **LLM-as-judge metrics** (require judge config):
    - relevance, faithfulness, completeness, coherence, safety, helpfulness, correctness,
    citation_quality

    Args:
        body (EvalRequest): Standalone evaluation request for POST /eval endpoint.
            Useful for testing evaluators without running a query.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[Error, EvalResult]
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
