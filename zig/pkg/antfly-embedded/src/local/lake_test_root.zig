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

pub const antfly_sources = @import("source_owner_storage.zig");

test {
    _ = @import("serverless/query/lake_serving.zig");
    _ = @import("serverless/query/lake_stream.zig");
    _ = @import("serverless/query/lake_parquet_rowgroup.zig");
    _ = @import("serverless/external_source/mod.zig");
    _ = @import("serverless/lake_host.zig");
    _ = @import("storage/rowsource/identity.zig");
    _ = @import("sql/lake_cursor.zig");
    _ = @import("sql/lake_values.zig");
}
