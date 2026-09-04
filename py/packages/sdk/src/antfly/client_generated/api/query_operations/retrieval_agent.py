from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.error import Error
from ...models.exact_sort_error import ExactSortError
from ...models.graph_anchor_filter_requires_index_error import GraphAnchorFilterRequiresIndexError
from ...models.graph_distinct_budget_exceeded_error import GraphDistinctBudgetExceededError
from ...models.graph_match_operation_limit_exceeded_error import GraphMatchOperationLimitExceededError
from ...models.graph_path_weight_domain_error import GraphPathWeightDomainError
from ...models.graph_query_unsupported_error import GraphQueryUnsupportedError
from ...models.graph_work_budget_exceeded_error import GraphWorkBudgetExceededError
from ...models.query_candidate_budget_exceeded_error import QueryCandidateBudgetExceededError
from ...models.query_dependency_error import QueryDependencyError
from ...models.query_filter_error import QueryFilterError
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...models.reranker_candidate_limit_exceeded_error import RerankerCandidateLimitExceededError
from ...models.retrieval_agent_request import RetrievalAgentRequest
from ...models.unsupported_hierarchy_grouping_error import UnsupportedHierarchyGroupingError
from ...models.unsupported_query_error import UnsupportedQueryError
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


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
    | None
):
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 404:
        response_404 = Error.from_dict(response.json())

        return response_404

    if response.status_code == 413:
        response_413 = QueryDependencyError.from_dict(response.json())

        return response_413

    if response.status_code == 422:

        def _parse_response_422(
            data: object,
        ) -> (
            ExactSortError
            | GraphAnchorFilterRequiresIndexError
            | GraphDistinctBudgetExceededError
            | GraphMatchOperationLimitExceededError
            | GraphPathWeightDomainError
            | GraphQueryUnsupportedError
            | GraphWorkBudgetExceededError
            | QueryCandidateBudgetExceededError
            | QueryDependencyError
            | QueryFilterError
            | RerankerCandidateLimitExceededError
            | UnsupportedHierarchyGroupingError
            | UnsupportedQueryError
        ):
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_0 = ExactSortError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_1 = QueryCandidateBudgetExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_2 = RerankerCandidateLimitExceededError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_0 = GraphDistinctBudgetExceededError.from_dict(
                    data
                )

                return componentsschemas_graph_query_unprocessable_error_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_1 = GraphWorkBudgetExceededError.from_dict(data)

                return componentsschemas_graph_query_unprocessable_error_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_2 = GraphPathWeightDomainError.from_dict(data)

                return componentsschemas_graph_query_unprocessable_error_type_2
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_3 = (
                    GraphAnchorFilterRequiresIndexError.from_dict(data)
                )

                return componentsschemas_graph_query_unprocessable_error_type_3
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_4 = GraphQueryUnsupportedError.from_dict(data)

                return componentsschemas_graph_query_unprocessable_error_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_unprocessable_error_type_5 = (
                    GraphMatchOperationLimitExceededError.from_dict(data)
                )

                return componentsschemas_graph_query_unprocessable_error_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_4 = QueryFilterError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_4
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_5 = UnsupportedHierarchyGroupingError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_5
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_query_unprocessable_error_type_6 = UnsupportedQueryError.from_dict(data)

                return componentsschemas_query_unprocessable_error_type_6
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_query_unprocessable_error_type_7 = QueryDependencyError.from_dict(data)

            return componentsschemas_query_unprocessable_error_type_7

        response_422 = _parse_response_422(response.json())

        return response_422

    if response.status_code == 429:
        response_429 = QueryDependencyError.from_dict(response.json())

        return response_429

    if response.status_code == 500:
        response_500 = Error.from_dict(response.json())

        return response_500

    if response.status_code == 502:
        response_502 = QueryDependencyError.from_dict(response.json())

        return response_502

    if response.status_code == 503:
        response_503 = QueryTemporarilyUnavailableError.from_dict(response.json())

        return response_503

    if response.status_code == 504:
        response_504 = QueryDependencyError.from_dict(response.json())

        return response_504

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
]:
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
) -> Response[
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
]:
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

            Authenticated row filters are enforced on every initial and generated
            operation in both modes, including scans, aggregates, and graph/tree
            traversal. They cannot be replaced or weakened by model tool arguments.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | QueryDependencyError | QueryTemporarilyUnavailableError | str]
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
) -> (
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
    | None
):
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

            Authenticated row filters are enforced on every initial and generated
            operation in both modes, including scans, aggregates, and graph/tree
            traversal. They cannot be replaced or weakened by model tool arguments.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | QueryDependencyError | QueryTemporarilyUnavailableError | str
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: RetrievalAgentRequest,
) -> Response[
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
]:
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

            Authenticated row filters are enforced on every initial and generated
            operation in both modes, including scans, aggregates, and graph/tree
            traversal. They cannot be replaced or weakened by model tool arguments.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | QueryDependencyError | QueryTemporarilyUnavailableError | str]
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
) -> (
    Error
    | ExactSortError
    | GraphAnchorFilterRequiresIndexError
    | GraphDistinctBudgetExceededError
    | GraphMatchOperationLimitExceededError
    | GraphPathWeightDomainError
    | GraphQueryUnsupportedError
    | GraphWorkBudgetExceededError
    | QueryCandidateBudgetExceededError
    | QueryDependencyError
    | QueryFilterError
    | RerankerCandidateLimitExceededError
    | UnsupportedHierarchyGroupingError
    | UnsupportedQueryError
    | QueryDependencyError
    | QueryTemporarilyUnavailableError
    | str
    | None
):
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

            Authenticated row filters are enforced on every initial and generated
            operation in both modes, including scans, aggregates, and graph/tree
            traversal. They cannot be replaced or weakened by model tool arguments.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | QueryDependencyError | QueryTemporarilyUnavailableError | str
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
