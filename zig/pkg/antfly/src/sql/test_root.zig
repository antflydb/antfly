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

test {
    _ = @import("aggregate_binding.zig");
    _ = @import("compiler.zig");
    _ = @import("scalar.zig");
    _ = @import("describe.zig");
    _ = @import("runtime.zig");
    _ = @import("insert_test.zig");
    _ = @import("returning_test.zig");
    _ = @import("catalog.zig");
    _ = @import("document_row.zig");
    _ = @import("operators.zig");
    _ = @import("plan_cache.zig");
    _ = @import("session.zig");
}
