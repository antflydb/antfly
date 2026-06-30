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

const public_embedding_query = @import("../query/public_embedding_query.zig");

pub const DenseEmbeddingQuery = public_embedding_query.DenseEmbeddingQuery;
pub const SparseEmbeddingQuery = public_embedding_query.SparseEmbeddingQuery;
pub const EmbeddingQuery = public_embedding_query.EmbeddingQuery;

pub const parseEmbeddingValueAlloc = public_embedding_query.parseEmbeddingValueAlloc;
pub const parseDenseEmbeddingAlloc = public_embedding_query.parseDenseEmbeddingAlloc;
pub const parseSparseEmbeddingAlloc = public_embedding_query.parseSparseEmbeddingAlloc;
