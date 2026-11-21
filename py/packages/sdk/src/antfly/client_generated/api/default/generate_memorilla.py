from http import HTTPStatus
from typing import Any

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.inference_error import InferenceError
from ...models.inference_memorilla_generate_request import InferenceMemorillaGenerateRequest
from ...models.inference_memorilla_generate_response import InferenceMemorillaGenerateResponse
from ...types import Response


def _get_kwargs(
    *,
    body: InferenceMemorillaGenerateRequest,
) -> dict[str, Any]:
    headers: dict[str, Any] = {}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": "/ai/v1/memorilla/generate",
    }

    _kwargs["json"] = body.to_dict()

    headers["Content-Type"] = "application/json"

    _kwargs["headers"] = headers
    return _kwargs


def _parse_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> InferenceError | InferenceMemorillaGenerateResponse | None:
    if response.status_code == 200:
        response_200 = InferenceMemorillaGenerateResponse.from_dict(response.json())

        return response_200

    if response.status_code == 400:
        response_400 = InferenceError.from_dict(response.json())

        return response_400

    if response.status_code == 401:
        response_401 = InferenceError.from_dict(response.json())

        return response_401

    if response.status_code == 404:
        response_404 = InferenceError.from_dict(response.json())

        return response_404

    if response.status_code == 408:
        response_408 = InferenceError.from_dict(response.json())

        return response_408

    if response.status_code == 409:
        response_409 = InferenceError.from_dict(response.json())

        return response_409

    if response.status_code == 413:
        response_413 = InferenceError.from_dict(response.json())

        return response_413

    if response.status_code == 500:
        response_500 = InferenceError.from_dict(response.json())

        return response_500

    if response.status_code == 503:
        response_503 = InferenceError.from_dict(response.json())

        return response_503

    if response.status_code == 504:
        response_504 = InferenceError.from_dict(response.json())

        return response_504

    if response.status_code == 507:
        response_507 = InferenceError.from_dict(response.json())

        return response_507

    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: AuthenticatedClient | Client, response: httpx.Response
) -> Response[InferenceError | InferenceMemorillaGenerateResponse]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceMemorillaGenerateRequest,
) -> Response[InferenceError | InferenceMemorillaGenerateResponse]:
    """Generate a memory answer with optional source verification

     Requires inference.memorilla.enabled and an administrator-configured
    profile. Runs the native question encoder, released memory module, and
    decoder with the qualified F32 CUDA arithmetic and fresh F32 KV.
    Responses are buffered. Streaming, sampling, caller-selected assets,
    prefix caches, and speculative decoding are not accepted by this route.
    Caller-supplied embeddings are a compatibility-bound model input, not
    proof of database authorization, freshness, provenance, or coverage.
    Explicit context_mode=source_verified also requires aligned document_texts.
    An independent source answer is generated without showing the memory
    candidate and must pass the model entailment check. The memory candidate
    can be published only if it agrees exactly after boundary whitespace
    trimming; otherwise the supported source answer is used. Incomplete,
    truncated or unsupported source answers cause abstention.
    Responses identify the final answer path and account
    for all internal generation and verification calls. A model support
    judgment is not a formal proof, and text/vector pairing is caller-supplied.

    Profiles configured with reader_kind=encoder_lora_research serve the
    experimental encoder-LoRA copy candidate. They take document_texts in
    latent mode, encode them server-side with the merged research encoder,
    reject document_embeddings (400) and source_verified (409), require the
    fixed research encoder identity, and report context_source
    supplied_documents with candidate_sha256. An answer-format guard
    replaces decoder text that is not a short fact (a few words of
    letters, digits, hyphen, period and apostrophe, or the literal
    UNKNOWN) with UNKNOWN and finish_reason abstained, reported in
    answer_guard. Every span copied into the memory record is returned in
    citations with its document_index and UTF-8 byte range [start, end)
    in that document_texts entry. This path is not qualified.

    Profiles configured with reader_kind=hybrid_research also take
    read_mode (auto, narrow or wide on a bundle with a span read cascade;
    none on any span bundle answers from the soft rows alone, i.e. stock
    Memorilla over the same pooled vectors) and include_diagnostics, which
    adds a diagnostics object (cascade margins and decision, gate, span and
    window as document byte ranges, the raw answer and stage timings).
    Other profile kinds reject both fields with 400.

    Args:
        body (InferenceMemorillaGenerateRequest): Generate a buffered answer over caller-supplied
            stored document embeddings.
            Features must be full-width normalized outputs of the configured encoder
            execution profile. The server applies the pinned FP16-storage/BF16-readout
            conversion. The identity pins compatibility; it does not authenticate the
            origin, authorization, or collection coverage of caller-supplied vectors.
            The first release supports greedy decoding with fresh F32 KV only.
            context_mode=source_verified generates an independent source answer and
            checks its support. A memory candidate requires exact agreement with that
            supported answer; otherwise the source answer or abstention is returned.
            This explicit mode does not establish
            source truth, authorization, or text/vector association cryptographically.
            Profiles configured with reader_kind=encoder_lora_research or
            hybrid_research instead take document_texts in latent mode and encode
            them server-side (the hybrid computes the pooled document and question
            vectors itself as well); they reject document_embeddings and
            source_verified.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceMemorillaGenerateResponse]
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
    client: AuthenticatedClient | Client,
    body: InferenceMemorillaGenerateRequest,
) -> InferenceError | InferenceMemorillaGenerateResponse | None:
    """Generate a memory answer with optional source verification

     Requires inference.memorilla.enabled and an administrator-configured
    profile. Runs the native question encoder, released memory module, and
    decoder with the qualified F32 CUDA arithmetic and fresh F32 KV.
    Responses are buffered. Streaming, sampling, caller-selected assets,
    prefix caches, and speculative decoding are not accepted by this route.
    Caller-supplied embeddings are a compatibility-bound model input, not
    proof of database authorization, freshness, provenance, or coverage.
    Explicit context_mode=source_verified also requires aligned document_texts.
    An independent source answer is generated without showing the memory
    candidate and must pass the model entailment check. The memory candidate
    can be published only if it agrees exactly after boundary whitespace
    trimming; otherwise the supported source answer is used. Incomplete,
    truncated or unsupported source answers cause abstention.
    Responses identify the final answer path and account
    for all internal generation and verification calls. A model support
    judgment is not a formal proof, and text/vector pairing is caller-supplied.

    Profiles configured with reader_kind=encoder_lora_research serve the
    experimental encoder-LoRA copy candidate. They take document_texts in
    latent mode, encode them server-side with the merged research encoder,
    reject document_embeddings (400) and source_verified (409), require the
    fixed research encoder identity, and report context_source
    supplied_documents with candidate_sha256. An answer-format guard
    replaces decoder text that is not a short fact (a few words of
    letters, digits, hyphen, period and apostrophe, or the literal
    UNKNOWN) with UNKNOWN and finish_reason abstained, reported in
    answer_guard. Every span copied into the memory record is returned in
    citations with its document_index and UTF-8 byte range [start, end)
    in that document_texts entry. This path is not qualified.

    Profiles configured with reader_kind=hybrid_research also take
    read_mode (auto, narrow or wide on a bundle with a span read cascade;
    none on any span bundle answers from the soft rows alone, i.e. stock
    Memorilla over the same pooled vectors) and include_diagnostics, which
    adds a diagnostics object (cascade margins and decision, gate, span and
    window as document byte ranges, the raw answer and stage timings).
    Other profile kinds reject both fields with 400.

    Args:
        body (InferenceMemorillaGenerateRequest): Generate a buffered answer over caller-supplied
            stored document embeddings.
            Features must be full-width normalized outputs of the configured encoder
            execution profile. The server applies the pinned FP16-storage/BF16-readout
            conversion. The identity pins compatibility; it does not authenticate the
            origin, authorization, or collection coverage of caller-supplied vectors.
            The first release supports greedy decoding with fresh F32 KV only.
            context_mode=source_verified generates an independent source answer and
            checks its support. A memory candidate requires exact agreement with that
            supported answer; otherwise the source answer or abstention is returned.
            This explicit mode does not establish
            source truth, authorization, or text/vector association cryptographically.
            Profiles configured with reader_kind=encoder_lora_research or
            hybrid_research instead take document_texts in latent mode and encode
            them server-side (the hybrid computes the pooled document and question
            vectors itself as well); they reject document_embeddings and
            source_verified.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceMemorillaGenerateResponse
    """

    return sync_detailed(
        client=client,
        body=body,
    ).parsed


async def asyncio_detailed(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceMemorillaGenerateRequest,
) -> Response[InferenceError | InferenceMemorillaGenerateResponse]:
    """Generate a memory answer with optional source verification

     Requires inference.memorilla.enabled and an administrator-configured
    profile. Runs the native question encoder, released memory module, and
    decoder with the qualified F32 CUDA arithmetic and fresh F32 KV.
    Responses are buffered. Streaming, sampling, caller-selected assets,
    prefix caches, and speculative decoding are not accepted by this route.
    Caller-supplied embeddings are a compatibility-bound model input, not
    proof of database authorization, freshness, provenance, or coverage.
    Explicit context_mode=source_verified also requires aligned document_texts.
    An independent source answer is generated without showing the memory
    candidate and must pass the model entailment check. The memory candidate
    can be published only if it agrees exactly after boundary whitespace
    trimming; otherwise the supported source answer is used. Incomplete,
    truncated or unsupported source answers cause abstention.
    Responses identify the final answer path and account
    for all internal generation and verification calls. A model support
    judgment is not a formal proof, and text/vector pairing is caller-supplied.

    Profiles configured with reader_kind=encoder_lora_research serve the
    experimental encoder-LoRA copy candidate. They take document_texts in
    latent mode, encode them server-side with the merged research encoder,
    reject document_embeddings (400) and source_verified (409), require the
    fixed research encoder identity, and report context_source
    supplied_documents with candidate_sha256. An answer-format guard
    replaces decoder text that is not a short fact (a few words of
    letters, digits, hyphen, period and apostrophe, or the literal
    UNKNOWN) with UNKNOWN and finish_reason abstained, reported in
    answer_guard. Every span copied into the memory record is returned in
    citations with its document_index and UTF-8 byte range [start, end)
    in that document_texts entry. This path is not qualified.

    Profiles configured with reader_kind=hybrid_research also take
    read_mode (auto, narrow or wide on a bundle with a span read cascade;
    none on any span bundle answers from the soft rows alone, i.e. stock
    Memorilla over the same pooled vectors) and include_diagnostics, which
    adds a diagnostics object (cascade margins and decision, gate, span and
    window as document byte ranges, the raw answer and stage timings).
    Other profile kinds reject both fields with 400.

    Args:
        body (InferenceMemorillaGenerateRequest): Generate a buffered answer over caller-supplied
            stored document embeddings.
            Features must be full-width normalized outputs of the configured encoder
            execution profile. The server applies the pinned FP16-storage/BF16-readout
            conversion. The identity pins compatibility; it does not authenticate the
            origin, authorization, or collection coverage of caller-supplied vectors.
            The first release supports greedy decoding with fresh F32 KV only.
            context_mode=source_verified generates an independent source answer and
            checks its support. A memory candidate requires exact agreement with that
            supported answer; otherwise the source answer or abstention is returned.
            This explicit mode does not establish
            source truth, authorization, or text/vector association cryptographically.
            Profiles configured with reader_kind=encoder_lora_research or
            hybrid_research instead take document_texts in latent mode and encode
            them server-side (the hybrid computes the pooled document and question
            vectors itself as well); they reject document_embeddings and
            source_verified.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[InferenceError | InferenceMemorillaGenerateResponse]
    """

    kwargs = _get_kwargs(
        body=body,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: AuthenticatedClient | Client,
    body: InferenceMemorillaGenerateRequest,
) -> InferenceError | InferenceMemorillaGenerateResponse | None:
    """Generate a memory answer with optional source verification

     Requires inference.memorilla.enabled and an administrator-configured
    profile. Runs the native question encoder, released memory module, and
    decoder with the qualified F32 CUDA arithmetic and fresh F32 KV.
    Responses are buffered. Streaming, sampling, caller-selected assets,
    prefix caches, and speculative decoding are not accepted by this route.
    Caller-supplied embeddings are a compatibility-bound model input, not
    proof of database authorization, freshness, provenance, or coverage.
    Explicit context_mode=source_verified also requires aligned document_texts.
    An independent source answer is generated without showing the memory
    candidate and must pass the model entailment check. The memory candidate
    can be published only if it agrees exactly after boundary whitespace
    trimming; otherwise the supported source answer is used. Incomplete,
    truncated or unsupported source answers cause abstention.
    Responses identify the final answer path and account
    for all internal generation and verification calls. A model support
    judgment is not a formal proof, and text/vector pairing is caller-supplied.

    Profiles configured with reader_kind=encoder_lora_research serve the
    experimental encoder-LoRA copy candidate. They take document_texts in
    latent mode, encode them server-side with the merged research encoder,
    reject document_embeddings (400) and source_verified (409), require the
    fixed research encoder identity, and report context_source
    supplied_documents with candidate_sha256. An answer-format guard
    replaces decoder text that is not a short fact (a few words of
    letters, digits, hyphen, period and apostrophe, or the literal
    UNKNOWN) with UNKNOWN and finish_reason abstained, reported in
    answer_guard. Every span copied into the memory record is returned in
    citations with its document_index and UTF-8 byte range [start, end)
    in that document_texts entry. This path is not qualified.

    Profiles configured with reader_kind=hybrid_research also take
    read_mode (auto, narrow or wide on a bundle with a span read cascade;
    none on any span bundle answers from the soft rows alone, i.e. stock
    Memorilla over the same pooled vectors) and include_diagnostics, which
    adds a diagnostics object (cascade margins and decision, gate, span and
    window as document byte ranges, the raw answer and stage timings).
    Other profile kinds reject both fields with 400.

    Args:
        body (InferenceMemorillaGenerateRequest): Generate a buffered answer over caller-supplied
            stored document embeddings.
            Features must be full-width normalized outputs of the configured encoder
            execution profile. The server applies the pinned FP16-storage/BF16-readout
            conversion. The identity pins compatibility; it does not authenticate the
            origin, authorization, or collection coverage of caller-supplied vectors.
            The first release supports greedy decoding with fresh F32 KV only.
            context_mode=source_verified generates an independent source answer and
            checks its support. A memory candidate requires exact agreement with that
            supported answer; otherwise the source answer or abstention is returned.
            This explicit mode does not establish
            source truth, authorization, or text/vector association cryptographically.
            Profiles configured with reader_kind=encoder_lora_research or
            hybrid_research instead take document_texts in latent mode and encode
            them server-side (the hybrid computes the pooled document and question
            vectors itself as well); they reject document_embeddings and
            source_verified.

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        InferenceError | InferenceMemorillaGenerateResponse
    """

    return (
        await asyncio_detailed(
            client=client,
            body=body,
        )
    ).parsed
