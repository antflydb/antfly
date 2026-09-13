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

const public_table_http = @import("api/public_table_http.zig");

test {
    _ = public_table_http;
    _ = @import("api/relational_rows.zig");
    _ = @import("api/http_route_helpers.zig");
    _ = @import("schema/relational_declarations.zig");
    _ = @import("api/table_reads.zig");
    _ = @import("api/relational_constraint_status.zig");
}
