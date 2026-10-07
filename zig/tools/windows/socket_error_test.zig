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

//! Qualification for Winsock error classification and Debug tracing.
const std = @import("std");
const errors = @import("antfly_platform").c.socket_testing;

test "Windows socket errors preserve recoverable connect failures" {
    if (@import("builtin").os.tag != .windows) return error.SkipZigTest;
    try std.testing.expectEqual(error.ConnectionResetByPeer, errors.connectError(10054));
    try std.testing.expectEqual(error.ConnectionResetByPeer, errors.connectError(10053));
    try std.testing.expectEqual(error.Timeout, errors.connectError(10060));
    try std.testing.expectEqual(error.ConnectionRefused, errors.connectError(10061));
    try std.testing.expectEqual(error.NetworkUnreachable, errors.connectError(10051));
    try std.testing.expectEqual(error.HostUnreachable, errors.connectError(10065));
    try std.testing.expectEqual(error.Canceled, errors.connectError(995));
}

test "Windows socket unknown errors remain numeric with tracing enabled" {
    if (@import("builtin").os.tag != .windows) return error.SkipZigTest;
    // Debug enables unexpected-error tracing. These unmapped Winsock values
    // must return Unexpected rather than format a nonexistent Win32 enum tag.
    try std.testing.expectEqual(error.Unexpected, errors.connectError(10022));
    try std.testing.expectEqual(error.Unexpected, errors.acceptError(10045));
    try std.testing.expectEqual(error.Unexpected, errors.streamError(10051));
}
