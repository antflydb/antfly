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

const public_search_request = @import("../query/public_search_request.zig");

pub const ParsedTextClauses = public_search_request.ParsedTextClauses;
pub const ParsedEmbedding = public_search_request.ParsedEmbedding;
pub const ParsedEmbeddings = public_search_request.ParsedEmbeddings;

pub const looksLikePublicSearchRequest = public_search_request.looksLikePublicSearchRequest;
pub const parseTextClausesAlloc = public_search_request.parseTextClausesAlloc;
pub const parseEmbeddingsAlloc = public_search_request.parseEmbeddingsAlloc;
pub const cloneRequestedIndexesAlloc = public_search_request.cloneRequestedIndexesAlloc;
pub const cloneRequestedFieldsAlloc = public_search_request.cloneRequestedFieldsAlloc;
pub const cloneFieldNamesAlloc = public_search_request.cloneFieldNamesAlloc;
