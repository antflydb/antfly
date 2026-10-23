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

pub const antfly_sources = @import("source_owner_storage.zig");
pub const common_bounded_admission = @import("common/bounded_admission.zig");
pub const common_request_admission = @import("common/request_admission.zig");
pub const consumer_tests_only = true;
test {
    _ = @import("storage/cache_close_admission_test.zig");
    _ = @import("storage/db/maintenance/producer_close_admission_test.zig");
    _ = @import("storage/db/maintenance/graph_metric_runtime.zig");
    _ = @import("storage/db/db.zig");
}
