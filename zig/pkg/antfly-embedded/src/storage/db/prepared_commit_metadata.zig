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

//! Borrowed native programs consumed inside one final writer attempt.
const admission = @import("../../common/read_admission.zig");
pub const Programs = struct {
    dense: ?*const @import("dense_artifact_counter_deltas.zig").Program = null,
    coverage: ?*const @import("prepared_coverage_outcomes.zig").Program = null,
    pub fn apply(self: Programs, writer: anytype, original: ?admission.Port) !void {
        if (self.dense) |program| try program.apply(writer, original);
        if (self.coverage) |program| try program.apply(writer, original);
    }
};
