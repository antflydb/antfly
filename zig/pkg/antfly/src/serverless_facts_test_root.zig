// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

test {
    _ = @import("serverless/hosted_reindex.zig");
    _ = @import("serverless/text_segment/paged.zig");
    _ = @import("serverless/sparse_segment/paged.zig");
    _ = @import("serverless/vector_segment/paged.zig");
    _ = @import("serverless/query/top_docs.zig");
    _ = @import("serverless/query/exact_scan.zig");
    _ = @import("serverless/build/external_source_plan_resolver.zig");
    _ = @import("serverless/build/external_source_publish.zig");
    _ = @import("serverless/build/external_source_read_authority.zig");
    _ = @import("serverless/build/document_facts.zig");
    _ = @import("serverless/build/document_facts_builder.zig");
    _ = @import("serverless/build/document_facts_publication_bench.zig");
    _ = @import("serverless/build/external_publication_metadata.zig");
    _ = @import("serverless/build/lake_rebuild.zig");
    _ = @import("serverless/search_sources.zig");
    _ = @import("serverless/graph_segment/page_bootstrap.zig");
    _ = @import("serverless/query/runtime.zig");
    _ = @import("serverless/query/document_facts_reader.zig");
    _ = @import("serverless/build/builder.zig");
    _ = @import("serverless/build/compactor.zig");
    _ = @import("serverless/catalog/service.zig");
    _ = @import("serverless/catalog/progress_store.zig");
    _ = @import("serverless/catalog/fs_progress_store.zig");
    _ = @import("serverless/catalog/object_progress_store.zig");
    _ = @import("serverless/enrichment/worker.zig");
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
