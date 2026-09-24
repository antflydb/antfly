from http import HTTPStatus
from typing import Any, cast

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
from ...models.inference_capacity_error import InferenceCapacityError
from ...models.query_candidate_budget_exceeded_error import QueryCandidateBudgetExceededError
from ...models.query_dependency_error import QueryDependencyError
from ...models.query_filter_error import QueryFilterError
from ...models.query_temporarily_unavailable_error import QueryTemporarilyUnavailableError
from ...models.reranker_candidate_limit_exceeded_error import RerankerCandidateLimitExceededError
from ...models.research_agent_request import ResearchAgentRequest
from ...models.unsupported_hierarchy_grouping_error import UnsupportedHierarchyGroupingError
from ...models.unsupported_query_error import UnsupportedQueryError
from ...types import Response


def _get_kwargs(
    *,
    body: ResearchAgentRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/db/v1/agents/research",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> (
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
    | str
    | None
):
    if response.status_code == 200:
        response_200 = response.text
        return response_200

    if response.status_code == 400:
        response_400 = Error.from_dict(response.json())

        return response_400

    if response.status_code == 403:
        response_403 = cast(Any, None)
        return response_403

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

        def _parse_response_503(data: object) -> InferenceCapacityError | QueryTemporarilyUnavailableError:
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                response_503_type_0 = QueryTemporarilyUnavailableError.from_dict(data)

                return response_503_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            response_503_type_1 = InferenceCapacityError.from_dict(data)

            return response_503_type_1

        response_503 = _parse_response_503(response.json())

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
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
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
    body: ResearchAgentRequest,
) -> Response[
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
    | str
]:
    """Research Agent - Bounded multi-phase research with a cited report

     Runs a bounded research state machine:
    plan → research (parallel retrieval researchers) → reflect → … → write → verify

    Every researcher is an ordinary retrieval-agent run over the request's
    authorized queries, so authorization, mandatory predicates and tool
    policy are identical to `/agents/retrieval`. Researchers return
    compressed findings; the writer only sees findings and a deduplicated
    evidence registry, and the server validates every `[E#]` citation.

    All work is bounded by `budget`. Send `research_state` back to resume
    or extend a run. For runs longer than one request, use
    `/agents/research/jobs`.

    **SSE Event Types:** the retrieval-agent events are reused.
    `step_progress` carries `phase` values `plan`, `sub_question_started`,
    `finding`, `reflection`, `section` and `verification`. Report text
    streams as `generation`. `done` carries the authoritative
    ResearchAgentResult.

    Args:
        body (ResearchAgentRequest): Request for the research agent. The agent plans sub-
            questions, runs a
            bounded retrieval researcher per sub-question in parallel, reflects on
            coverage, and writes a long-form report whose `[E#]` citations resolve
            to a deduplicated evidence registry.

            Researchers are ordinary retrieval-agent runs over `queries` with the
            same authorization, mandatory predicates and tool policy. They cannot
            widen tables, filters, tools or budgets.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | str]
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
    body: ResearchAgentRequest,
) -> (
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
    | str
    | None
):
    """Research Agent - Bounded multi-phase research with a cited report

     Runs a bounded research state machine:
    plan → research (parallel retrieval researchers) → reflect → … → write → verify

    Every researcher is an ordinary retrieval-agent run over the request's
    authorized queries, so authorization, mandatory predicates and tool
    policy are identical to `/agents/retrieval`. Researchers return
    compressed findings; the writer only sees findings and a deduplicated
    evidence registry, and the server validates every `[E#]` citation.

    All work is bounded by `budget`. Send `research_state` back to resume
    or extend a run. For runs longer than one request, use
    `/agents/research/jobs`.

    **SSE Event Types:** the retrieval-agent events are reused.
    `step_progress` carries `phase` values `plan`, `sub_question_started`,
    `finding`, `reflection`, `section` and `verification`. Report text
    streams as `generation`. `done` carries the authoritative
    ResearchAgentResult.

    Args:
        body (ResearchAgentRequest): Request for the research agent. The agent plans sub-
            questions, runs a
            bounded retrieval researcher per sub-question in parallel, reflects on
            coverage, and writes a long-form report whose `[E#]` citations resolve
            to a deduplicated evidence registry.

            Researchers are ordinary retrieval-agent runs over `queries` with the
            same authorization, mandatory predicates and tool policy. They cannot
            widen tables, filters, tools or budgets.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | str
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient,
    body: ResearchAgentRequest,
) -> Response[
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
    | str
]:
    """Research Agent - Bounded multi-phase research with a cited report

     Runs a bounded research state machine:
    plan → research (parallel retrieval researchers) → reflect → … → write → verify

    Every researcher is an ordinary retrieval-agent run over the request's
    authorized queries, so authorization, mandatory predicates and tool
    policy are identical to `/agents/retrieval`. Researchers return
    compressed findings; the writer only sees findings and a deduplicated
    evidence registry, and the server validates every `[E#]` citation.

    All work is bounded by `budget`. Send `research_state` back to resume
    or extend a run. For runs longer than one request, use
    `/agents/research/jobs`.

    **SSE Event Types:** the retrieval-agent events are reused.
    `step_progress` carries `phase` values `plan`, `sub_question_started`,
    `finding`, `reflection`, `section` and `verification`. Report text
    streams as `generation`. `done` carries the authoritative
    ResearchAgentResult.

    Args:
        body (ResearchAgentRequest): Request for the research agent. The agent plans sub-
            questions, runs a
            bounded retrieval researcher per sub-question in parallel, reflects on
            coverage, and writes a long-form report whose `[E#]` citations resolve
            to a deduplicated evidence registry.

            Researchers are ordinary retrieval-agent runs over `queries` with the
            same authorization, mandatory predicates and tool policy. They cannot
            widen tables, filters, tools or budgets.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Any | Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | str]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient,
    body: ResearchAgentRequest,
) -> (
    Any
    | Error
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
    | InferenceCapacityError
    | QueryTemporarilyUnavailableError
    | QueryDependencyError
    | str
    | None
):
    """Research Agent - Bounded multi-phase research with a cited report

     Runs a bounded research state machine:
    plan → research (parallel retrieval researchers) → reflect → … → write → verify

    Every researcher is an ordinary retrieval-agent run over the request's
    authorized queries, so authorization, mandatory predicates and tool
    policy are identical to `/agents/retrieval`. Researchers return
    compressed findings; the writer only sees findings and a deduplicated
    evidence registry, and the server validates every `[E#]` citation.

    All work is bounded by `budget`. Send `research_state` back to resume
    or extend a run. For runs longer than one request, use
    `/agents/research/jobs`.

    **SSE Event Types:** the retrieval-agent events are reused.
    `step_progress` carries `phase` values `plan`, `sub_question_started`,
    `finding`, `reflection`, `section` and `verification`. Report text
    streams as `generation`. `done` carries the authoritative
    ResearchAgentResult.

    Args:
        body (ResearchAgentRequest): Request for the research agent. The agent plans sub-
            questions, runs a
            bounded retrieval researcher per sub-question in parallel, reflects on
            coverage, and writes a long-form report whose `[E#]` citations resolve
            to a deduplicated evidence registry.

            Researchers are ordinary retrieval-agent runs over `queries` with the
            same authorization, mandatory predicates and tool policy. They cannot
            widen tables, filters, tools or budgets.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Any | Error | ExactSortError | GraphAnchorFilterRequiresIndexError | GraphDistinctBudgetExceededError | GraphMatchOperationLimitExceededError | GraphPathWeightDomainError | GraphQueryUnsupportedError | GraphWorkBudgetExceededError | QueryCandidateBudgetExceededError | QueryDependencyError | QueryFilterError | RerankerCandidateLimitExceededError | UnsupportedHierarchyGroupingError | UnsupportedQueryError | InferenceCapacityError | QueryTemporarilyUnavailableError | QueryDependencyError | str
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
