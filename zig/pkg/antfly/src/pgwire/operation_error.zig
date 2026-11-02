// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Translate backend cancellation at the owning frontend's error boundary.
const std = @import("std");

/// Backend workers and callbacks can cancel independently of the connection.
/// Only the frontend's own canceled I/O may use the protocol shutdown signal.
/// This changes only the wire error identity; ownership and cleanup stay with
/// the caller and its retained-resource owners.
pub fn fromBackend(frontend_io: std.Io, failure: anyerror) anyerror {
    if (failure == error.Canceled or failure == error.Cancelled) {
        frontend_io.checkCancel() catch return error.Canceled;
        return error.QueryCanceled;
    }
    return failure;
}

test "pgwire backend cancellation keeps a healthy frontend reusable" {
    try std.testing.expectEqual(error.QueryCanceled, fromBackend(std.testing.io, error.Canceled));
    try std.testing.expectEqual(error.QueryCanceled, fromBackend(std.testing.io, error.Cancelled));
}

test "pgwire backend cancellation preserves actual frontend shutdown" {
    const Canceled = struct {
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            return error.Canceled;
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Canceled.check;
    var frontend_io = std.testing.io;
    frontend_io.vtable = &vtable;
    try std.testing.expectEqual(error.Canceled, fromBackend(frontend_io, error.Canceled));
    try std.testing.expectEqual(error.Canceled, fromBackend(frontend_io, error.Cancelled));
}

test "pgwire backend error translation preserves other failure identities" {
    const Unexpected = struct {
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            @panic("unrelated backend failure queried transport cancellation");
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Unexpected.check;
    var frontend_io = std.testing.io;
    frontend_io.vtable = &vtable;
    for ([_]anyerror{ error.ReadMemoryCapacityUnavailable, error.OutOfMemory, error.RemoteReadUnavailable, error.Forbidden, error.QueryCanceled, error.DeadlineExceeded }) |failure| {
        try std.testing.expectEqual(failure, fromBackend(frontend_io, failure));
    }
}
