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

//! Consumer tests intentionally have no physical-index source import.
const std = @import("std");
const abi = @import("architecture_probe_abi");
const identity = @import("runtime_failure_identity");

test "compiled index owner creates and destroys exact production index" {
    const name = "probe";
    const config = "{\"version\":1}";
    var handle: ?*anyopaque = null;
    var failure: abi.failure.FailureIdentity = .{};
    const status = abi.antfly_probe_index_open(&.{ .name = .{ .ptr = name.ptr, .len = name.len }, .config = .{ .ptr = config.ptr, .len = config.len } }, &handle, &failure);
    try identity.validateFailureEnvelope(status, &failure, abi.version);
    try identity.statusToError(status);
    defer abi.antfly_probe_index_close(handle);
    try std.testing.expect(handle != null);
}

test "compiled index owner preserves exact config failure and clears output" {
    const name = "probe";
    const config = "{\"version\":9}";
    var handle: ?*anyopaque = null;
    var failure: abi.failure.FailureIdentity = .{};
    const status = abi.antfly_probe_index_open(&.{ .name = .{ .ptr = name.ptr, .len = name.len }, .config = .{ .ptr = config.ptr, .len = config.len } }, &handle, &failure);
    try identity.validateFailureEnvelope(status, &failure, abi.version);
    try std.testing.expectError(error.InvalidAlgebraicConfig, identity.statusToError(status));
    try std.testing.expectEqualStrings("InvalidAlgebraicConfig", failure.errorName());
    try std.testing.expect(handle == null);
}
