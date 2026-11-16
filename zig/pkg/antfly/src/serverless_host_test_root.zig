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
    _ = @import("serverless/hosted_embedding_checkpoint.zig");
    _ = @import("serverless/query/live_view.zig");
    _ = @import("serverless/query/live_overlay.zig");
    _ = @import("serverless/query/live_filter.zig");
    _ = @import("serverless/visibility.zig");
    _ = @import("serverless/wal/indexed_object_store.zig");
}

pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
