// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const query_contract = @import("../query/contract.zig");

pub const QueryResponse = query_contract.QueryResponse;
pub const testing = query_contract.testing;
pub const QueryResponseMeta = query_contract.QueryResponseMeta;
pub const NativeDocIdConstraintEnvelope = query_contract.NativeDocIdConstraintEnvelope;
pub const OwnedNativeDocIdConstraintEnvelope = query_contract.OwnedNativeDocIdConstraintEnvelope;
pub const AlgebraicVectorWorkerRequestOptions = query_contract.AlgebraicVectorWorkerRequestOptions;
pub const OwnedAlgebraicVectorWorkerRequestEnvelope = query_contract.OwnedAlgebraicVectorWorkerRequestEnvelope;
pub const AlgebraicVectorWorkerQuery = query_contract.AlgebraicVectorWorkerQuery;
pub const OwnedAlgebraicVectorWorkerQuery = query_contract.OwnedAlgebraicVectorWorkerQuery;
pub const AlgebraicTensorAccessPathEnvelopeInput = query_contract.AlgebraicTensorAccessPathEnvelopeInput;
pub const AlgebraicDictionaryIdentityInput = query_contract.AlgebraicDictionaryIdentityInput;
pub const AlgebraicTensorExprEnvelopeInput = query_contract.AlgebraicTensorExprEnvelopeInput;
pub const AlgebraicTensorProgramRefInput = query_contract.AlgebraicTensorProgramRefInput;
pub const AlgebraicTensorProgramStepEnvelopeInput = query_contract.AlgebraicTensorProgramStepEnvelopeInput;
pub const AlgebraicTensorProgramEnvelopeInput = query_contract.AlgebraicTensorProgramEnvelopeInput;
pub const OwnedAlgebraicTensorAccessPathEnvelope = query_contract.OwnedAlgebraicTensorAccessPathEnvelope;
pub const OwnedAlgebraicDictionaryIdentity = query_contract.OwnedAlgebraicDictionaryIdentity;
pub const OwnedAlgebraicTensorExprEnvelope = query_contract.OwnedAlgebraicTensorExprEnvelope;
pub const OwnedAlgebraicTensorProgramStepEnvelope = query_contract.OwnedAlgebraicTensorProgramStepEnvelope;
pub const OwnedAlgebraicTensorProgramEnvelope = query_contract.OwnedAlgebraicTensorProgramEnvelope;
pub const OwnedAlgebraicTensorProgramView = query_contract.OwnedAlgebraicTensorProgramView;
pub const OwnedQueryRequest = query_contract.OwnedQueryRequest;
pub const QueryPreflightSummary = query_contract.QueryPreflightSummary;
pub const SemanticResolver = query_contract.SemanticResolver;

pub const nativeDocIdConstraintEnvelopeFromSearchRequest = query_contract.nativeDocIdConstraintEnvelopeFromSearchRequest;
pub const encodeAlgebraicTensorAccessPathEnvelopeAlloc = query_contract.encodeAlgebraicTensorAccessPathEnvelopeAlloc;
pub const parseAlgebraicTensorAccessPathEnvelopeAlloc = query_contract.parseAlgebraicTensorAccessPathEnvelopeAlloc;
pub const encodeAlgebraicTensorExprEnvelopeAlloc = query_contract.encodeAlgebraicTensorExprEnvelopeAlloc;
pub const encodeAlgebraicTensorProgramEnvelopeAlloc = query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc;
pub const parseAlgebraicTensorExprEnvelopeAlloc = query_contract.parseAlgebraicTensorExprEnvelopeAlloc;
pub const parseAlgebraicTensorExprEnvelopeInputAlloc = query_contract.parseAlgebraicTensorExprEnvelopeInputAlloc;
pub const parseAlgebraicTensorProgramEnvelopeAlloc = query_contract.parseAlgebraicTensorProgramEnvelopeAlloc;
pub const parseAlgebraicTensorProgramEnvelopeInputAlloc = query_contract.parseAlgebraicTensorProgramEnvelopeInputAlloc;
pub const encodeAlgebraicVectorWorkerRequestEnvelopeAlloc = query_contract.encodeAlgebraicVectorWorkerRequestEnvelopeAlloc;
pub const parseAlgebraicVectorWorkerRequestEnvelopeAlloc = query_contract.parseAlgebraicVectorWorkerRequestEnvelopeAlloc;
pub const parseAlgebraicTensorAccessPathEnvelopeInputAlloc = query_contract.parseAlgebraicTensorAccessPathEnvelopeInputAlloc;
pub const applyNativeDocIdConstraintEnvelope = query_contract.applyNativeDocIdConstraintEnvelope;
pub const encodeNativeDocIdConstraintEnvelopeAlloc = query_contract.encodeNativeDocIdConstraintEnvelopeAlloc;
pub const parseNativeDocIdConstraintEnvelopeAlloc = query_contract.parseNativeDocIdConstraintEnvelopeAlloc;
pub const parseQueryRequest = query_contract.parseQueryRequest;
pub const parsePublicQueryRequest = query_contract.parsePublicQueryRequest;
pub const preflightGraphSearchesAlloc = query_contract.preflightGraphSearchesAlloc;
pub const preflightQueryRequestAlloc = query_contract.preflightQueryRequestAlloc;
pub const encodeQueryResponses = query_contract.encodeQueryResponses;
pub const parseAggregationRequestsJson = query_contract.parseAggregationRequestsJson;
pub const freeAggregationRequests = query_contract.freeAggregationRequests;
pub const encodeSupportedPatternFilterQueryAlloc = query_contract.encodeSupportedPatternFilterQueryAlloc;
