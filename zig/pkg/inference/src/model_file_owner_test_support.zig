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

//! Focused native file integration entry with the production checkpoint TLS.
pub const testFilePhysicalOwnership = @import("util/c_file.zig").testFilePhysicalOwnership;

/// Exercise receipt validation and content verification with the production TLS
/// while the API test supplies the actual scheduler owner.
pub fn testArtifactPhysicalOwnership(alloc: @import("std").mem.Allocator, io: @import("std").Io, path: []const u8, context: *anyopaque, check: *const fn (*anyopaque) anyerror!void, park: *const fn (*anyopaque) anyerror!void, enter: *const fn (*anyopaque) anyerror!void) !u64 {
    const builtin = @import("builtin");
    if (!builtin.is_test) @compileError("test only");
    const execution = @import("execution_control.zig");
    const receipt = @import("registry/managed_receipt.zig");
    var scope = execution.CheckpointScope.init(.{ .context = context, .check_fn = check, .wait_begin_fn = park, .wait_end_fn = enter });
    defer scope.deinit();
    var validated = (try receipt.loadValidated(alloc, io, path)) orelse return error.MissingReceipt;
    defer validated.deinit();
    for (validated.artifacts) |*artifact| try receipt.verifyValidatedArtifactSha256(io, artifact, artifact.sha256.?);
    return validated.artifacts[0].size;
}
