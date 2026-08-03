from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.retrieval_agent_request import RetrievalAgentRequest
from ...types import Response


def _get_kwargs(
    *,
    body: RetrievalAgentRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/agents/retrieval",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Error | str | None:
    if response.status_code == 200:
        response_200 = response.text
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


def _build_response(*, client: AuthenticatedClient | Client, response: httpx.Response) -> Response[Error | str]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient,
    body: RetrievalAgentRequest,
) -> Response[Error | str]:
    """Retrieval Agent - Agentic document retrieval with tool calling

     Uses a DFA-based approach to retrieve documents:
    clarify → select_strategy → refine_query → execute

    **Key Features:**
    - **Multi-strategy**: Semantic, BM25, tree, graph, metadata, or hybrid
    - **Query Pipeline**: Chain queries with references (e.g., tree search starting from semantic
    results)
    - **Clarification**: Optional multi-turn for query disambiguation
    - **Reasoning Chain**: Returns steps taken during retrieval

    **Strategies:**
    - `semantic`: Vector similarity search using embeddings
    - `bm25`: Full-text search with BM25 scoring
    - `metadata`: Structured field queries
    - `tree`: Iterative tree navigation with summarization (PageIndex-style)
    - `graph`: Relationship-based traversal
    - `hybrid`: Combine strategies with RRF or rerank

    **SSE Event Types:**
    - `step_started`: Pipeline step began (see SSEStepStarted schema)
    - `step_progress`: Progress within a step (see SSEStepProgress schema)
    - `step_completed`: Pipeline step finished (see SSEStepCompleted schema)
    - `classification`: Query classification result (see ClassificationTransformationResult)
    - `reasoning`: Streamed reasoning text chunk (string)
    - `followup`: Generated follow-up question (string)
    - `hit`: Individual document result (see QueryHit)
    - `tool_mode`: Tool calling mode selected (see SSEToolMode)
    - `eval`: Evaluation metrics (see EvalResult)
    - `done`: Retrieval complete (see RetrievalAgentResult)
    - `done` is the authoritative final bounded-agent envelope for both JSON and SSE consumers
    - `error`: Error occurred (see SSEError)

    Args:
        body (RetrievalAgentRequest): Request for the retrieval agent. Queries define which tables
            and indexes
            to search, each as a QueryRequest with optional tree search configuration.

            **Pipeline mode** (default, max_internal_iterations=0): Queries are executed
            directly without an LLM tool-calling loop.

            **Agentic mode** (max_internal_iterations > 0): The LLM decides which tools to
            call, using the queries to determine available tables and indexes.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | str]
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
    body: RetrievalAgentRequest,
) -> Error | str | None:
    """Retrieval Agent - Agentic document retrieval with tool calling

     Uses a DFA-based approach to retrieve documents:
    clarify → select_strategy → refine_query → execute

    **Key Features:**
    - **Multi-strategy**: Semantic, BM25, tree, graph, metadata, or hybrid
    - **Query Pipeline**: Chain queries with references (e.g., tree search starting from semantic
    results)
    - **Clarification**: Optional multi-turn for query disambiguation
    - **Reasoning Chain**: Returns steps taken during retrieval

    **Strategies:**
    - `semantic`: Vector similarity search using embeddings
    - `bm25`: Full-text search with BM25 scoring
    - `metadata`: Structured field queries
    - `tree`: Iterative tree navigation with summarization (PageIndex-style)
    - `graph`: Relationship-based traversal
    - `hybrid`: Combine strategies with RRF or rerank

    **SSE Event Types:**
    - `step_started`: Pipeline step began (see SSEStepStarted schema)
    - `step_progress`: Progress within a step (see SSEStepProgress schema)
    - `step_completed`: Pipeline step finished (see SSEStepCompleted schema)
    - `classification`: Query classification result (see ClassificationTransformationResult)
    - `reasoning`: Streamed reasoning text chunk (string)
    - `followup`: Generated follow-up question (string)
    - `hit`: Individual document result (see QueryHit)
    - `tool_mode`: Tool calling mode selected (see SSEToolMode)
    - `eval`: Evaluation metrics (see EvalResult)
    - `done`: Retrieval complete (see RetrievalAgentResult)
    - `done` is the authoritative final bounded-agent envelope for both JSON and SSE consumers
    - `error`: Error occurred (see SSEError)

    Args:
        body (RetrievalAgentRequest): Request for the retrieval agent. Queries define which tables
            and indexes
            to search, each as a QueryRequest with optional tree search configuration.

            **Pipeline mode** (default, max_internal_iterations=0): Queries are executed
            directly without an LLM tool-calling loop.

            **Agentic mode** (max_internal_iterations > 0): The LLM decides which tools to
            call, using the queries to determine available tables and indexes.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | str
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: RetrievalAgentRequest,
) -> Response[Error | str]:
    """Retrieval Agent - Agentic document retrieval with tool calling

     Uses a DFA-based approach to retrieve documents:
    clarify → select_strategy → refine_query → execute

    **Key Features:**
    - **Multi-strategy**: Semantic, BM25, tree, graph, metadata, or hybrid
    - **Query Pipeline**: Chain queries with references (e.g., tree search starting from semantic
    results)
    - **Clarification**: Optional multi-turn for query disambiguation
    - **Reasoning Chain**: Returns steps taken during retrieval

    **Strategies:**
    - `semantic`: Vector similarity search using embeddings
    - `bm25`: Full-text search with BM25 scoring
    - `metadata`: Structured field queries
    - `tree`: Iterative tree navigation with summarization (PageIndex-style)
    - `graph`: Relationship-based traversal
    - `hybrid`: Combine strategies with RRF or rerank

    **SSE Event Types:**
    - `step_started`: Pipeline step began (see SSEStepStarted schema)
    - `step_progress`: Progress within a step (see SSEStepProgress schema)
    - `step_completed`: Pipeline step finished (see SSEStepCompleted schema)
    - `classification`: Query classification result (see ClassificationTransformationResult)
    - `reasoning`: Streamed reasoning text chunk (string)
    - `followup`: Generated follow-up question (string)
    - `hit`: Individual document result (see QueryHit)
    - `tool_mode`: Tool calling mode selected (see SSEToolMode)
    - `eval`: Evaluation metrics (see EvalResult)
    - `done`: Retrieval complete (see RetrievalAgentResult)
    - `done` is the authoritative final bounded-agent envelope for both JSON and SSE consumers
    - `error`: Error occurred (see SSEError)

    Args:
        body (RetrievalAgentRequest): Request for the retrieval agent. Queries define which tables
            and indexes
            to search, each as a QueryRequest with optional tree search configuration.

            **Pipeline mode** (default, max_internal_iterations=0): Queries are executed
            directly without an LLM tool-calling loop.

            **Agentic mode** (max_internal_iterations > 0): The LLM decides which tools to
            call, using the queries to determine available tables and indexes.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | str]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: RetrievalAgentRequest,
) -> Error | str | None:
    """Retrieval Agent - Agentic document retrieval with tool calling

     Uses a DFA-based approach to retrieve documents:
    clarify → select_strategy → refine_query → execute

    **Key Features:**
    - **Multi-strategy**: Semantic, BM25, tree, graph, metadata, or hybrid
    - **Query Pipeline**: Chain queries with references (e.g., tree search starting from semantic
    results)
    - **Clarification**: Optional multi-turn for query disambiguation
    - **Reasoning Chain**: Returns steps taken during retrieval

    **Strategies:**
    - `semantic`: Vector similarity search using embeddings
    - `bm25`: Full-text search with BM25 scoring
    - `metadata`: Structured field queries
    - `tree`: Iterative tree navigation with summarization (PageIndex-style)
    - `graph`: Relationship-based traversal
    - `hybrid`: Combine strategies with RRF or rerank

    **SSE Event Types:**
    - `step_started`: Pipeline step began (see SSEStepStarted schema)
    - `step_progress`: Progress within a step (see SSEStepProgress schema)
    - `step_completed`: Pipeline step finished (see SSEStepCompleted schema)
    - `classification`: Query classification result (see ClassificationTransformationResult)
    - `reasoning`: Streamed reasoning text chunk (string)
    - `followup`: Generated follow-up question (string)
    - `hit`: Individual document result (see QueryHit)
    - `tool_mode`: Tool calling mode selected (see SSEToolMode)
    - `eval`: Evaluation metrics (see EvalResult)
    - `done`: Retrieval complete (see RetrievalAgentResult)
    - `done` is the authoritative final bounded-agent envelope for both JSON and SSE consumers
    - `error`: Error occurred (see SSEError)

    Args:
        body (RetrievalAgentRequest): Request for the retrieval agent. Queries define which tables
            and indexes
            to search, each as a QueryRequest with optional tree search configuration.

            **Pipeline mode** (default, max_internal_iterations=0): Queries are executed
            directly without an LLM tool-calling loop.

            **Agentic mode** (max_internal_iterations > 0): The LLM decides which tools to
            call, using the queries to determine available tables and indexes.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | str
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
