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

pub const antfly_sources = @import("source_owner_physical.zig");

test {
    _ = @import("api/lake_metadata_memo.zig");
    _ = @import("api/lake_index_refinements_test.zig");
    _ = @import("api/lake_source_navigation.zig");
    _ = @import("api/lake_retained_cut.zig");
    _ = @import("api/lake_index_publication.zig");
    _ = @import("api/lake_index_coordinator.zig");
    _ = @import("api/lake_recent_vectors.zig");
    _ = @import("serverless/artifacts/cloud_generations.zig");
    _ = @import("api/lake_generation_envelope.zig");
    _ = @import("api/lake_index_serving_compaction.zig");
    _ = @import("api/lake_index_text_spans.zig");
    _ = @import("api/lake_artifact_wave.zig");
    _ = @import("api/lake_index_sort_bounds.zig");
    _ = @import("api/lake_cloud_publication.zig");
    _ = @import("api/lake_index_gc_progress.zig");
    _ = @import("api/http_server.zig");
    _ = @import("api/lake_text_window_ahead.zig");
    _ = @import("api/lake_text_pack_wave.zig");
    _ = @import("api/lake_search_overlay.zig");
    _ = @import("api/lake_recent_text_cache.zig");
    _ = @import("api/lake_wal_cut_cache.zig");
    _ = @import("api/lake_index_source_routes.zig");
    _ = @import("api/lake_index_text_frontier.zig");
    _ = @import("api/lake_index_text_query.zig");
    _ = @import("antfly_local_sources").search_search;
    _ = @import("antfly_local_sources").storage_typed_json;
    _ = @import("antfly_local_sources").encoding_roaring;
}
