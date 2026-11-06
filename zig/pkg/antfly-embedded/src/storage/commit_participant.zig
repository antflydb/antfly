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

//! Borrowed source-stamped commit participation across storage type erasure.
//! Empty/control-only writes and inactive source authorities do not invoke the
//! final callback; this is not a general post-commit notification mechanism.
//! The participant
//! cannot commit or abort its host. Its final callback may stage only private
//! metadata, after the host has stamped the observed mutations but before the
//! physical commit. Callback failure aborts the entire host transaction.

/// One mutation into a `View`'s index-catalog write surface (see
/// `View.putIndexCatalog`). Mirrors `native_catalog_backend.Mutation`
/// exactly (namespace-scoped, backend-agnostic); kept as a distinct type
/// here only so this file never needs to import a Lite-specific module.
pub const IndexCatalogMutation = @import("native_catalog_backend.zig").Mutation;

/// Unwraps every level of pointer indirection. `View.from`'s `txn:
/// anytype` argument is sometimes a single pointer (`&concrete_txn`) and
/// sometimes a pointer-to-a-copied-pointer (callers that themselves
/// received `txn: anytype` as a pointer and pass `&that_copy` on,
/// e.g. `artifact_input_capture.zig`'s `Capture.stage`), so `@hasDecl`
/// needs the base struct type, not whatever single level `@typeInfo`
/// would otherwise give.
fn BaseType(comptime T: type) type {
    return switch (@typeInfo(T)) {
        .pointer => |info| BaseType(info.child),
        else => T,
    };
}

pub const View = struct {
    ptr: *anyopaque,
    get_fn: *const fn (*anyopaque, []const u8) anyerror![]const u8,
    put_fn: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
    /// WS4 phase 2: optional second write surface, present only when the
    /// host transaction's backend can also durably place records in its
    /// index-catalog root within this SAME native transaction (Lite's
    /// concrete `Txn`; `null` on every other backend). `scope` is the
    /// publishing `PersistentIndex`'s own catalog scope (its index path),
    /// exactly as `native_catalog_backend.Options.scope`/
    /// `native_catalog_sink.zig`'s `Sink.submit` already use.
    put_index_catalog_fn: ?*const fn (*anyopaque, scope: []const u8, mutations: []const IndexCatalogMutation) anyerror!void = null,

    pub fn get(self: View, key: []const u8) ![]const u8 {
        return self.get_fn(self.ptr, key);
    }

    pub fn put(self: View, key: []const u8, value: []const u8) !void {
        // Public rows and derived artifacts must not change after their
        // revision stamps were finalized. Only the private metadata namespace
        // is writable through this restricted commit view.
        if (key.len < 2 or key[0] != 0 or key[1] != 0) return error.InvalidCommitMetadata;
        return self.put_fn(self.ptr, key, value);
    }

    /// Stage `mutations` into the host transaction's index-catalog root
    /// (a catalog root distinct from the primary keyspace `put` above),
    /// applied in the SAME native transaction once the host commits.
    /// `error.IndexCatalogUnsupported` when the host transaction has no
    /// such second catalog root (every backend but Lite).
    pub fn putIndexCatalog(self: View, scope: []const u8, mutations: []const IndexCatalogMutation) !void {
        const put_index_catalog = self.put_index_catalog_fn orelse return error.IndexCatalogUnsupported;
        return put_index_catalog(self.ptr, scope, mutations);
    }

    /// The view and every returned slice are borrowed only through callback
    /// return. It carries no allocation, ownership, or transaction lifecycle.
    pub fn from(txn: anytype) View {
        const T = @TypeOf(txn);
        const Bridge = struct {
            fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.get(key);
            }
            fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.put(key, value);
            }
            fn putIndexCatalog(ptr: *anyopaque, scope: []const u8, mutations: []const IndexCatalogMutation) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.putIndexCatalogBatch(scope, mutations);
            }
        };
        return .{
            .ptr = @ptrCast(txn),
            .get_fn = Bridge.get,
            .put_fn = Bridge.put,
            .put_index_catalog_fn = if (@hasDecl(BaseType(T), "putIndexCatalogBatch")) Bridge.putIndexCatalog else null,
        };
    }
};

pub const Participant = struct {
    ptr: *anyopaque,
    /// Called on attachment to a fresh writer attempt, before any mutation.
    reset: *const fn (*anyopaque) void,
    observe: *const fn (*anyopaque, []const u8, ?[]const u8) void,
    /// The host's source-position encoding is opaque to the backend erasure
    /// layer. A participant must validate the encoding it expects.
    stage: *const fn (*anyopaque, View, []const u8) anyerror!void,
};

/// WS4 phase 2: a short, fixed-capacity list of contributors sharing one
/// attachment slot (`artifact_input_capture.zig`'s `Capture.participant` is
/// still exactly one `?Participant`). Composes several independent
/// `Participant`s -- e.g. the existing authored-acceptance stamper plus a
/// new text-index publisher -- behind the single `Participant` interface
/// every attachment point already expects, so none of that plumbing needs
/// to change. Every member's `stage` runs against the SAME `View`, in
/// attachment order; a failure from any member aborts the whole host
/// transaction, same as a single `Participant` failing today.
pub const List = struct {
    pub const max_participants = 4;

    items: [max_participants]Participant = undefined,
    len: usize = 0,

    pub fn append(self: *List, new_participant: Participant) !void {
        if (self.len >= max_participants) return error.TooManyCommitParticipants;
        self.items[self.len] = new_participant;
        self.len += 1;
    }

    fn reset(ptr: *anyopaque) void {
        const self: *List = @ptrCast(@alignCast(ptr));
        for (self.items[0..self.len]) |p| p.reset(p.ptr);
    }

    fn observe(ptr: *anyopaque, key: []const u8, value: ?[]const u8) void {
        const self: *List = @ptrCast(@alignCast(ptr));
        for (self.items[0..self.len]) |p| p.observe(p.ptr, key, value);
    }

    fn stage(ptr: *anyopaque, view: View, encoded_position: []const u8) anyerror!void {
        const self: *List = @ptrCast(@alignCast(ptr));
        for (self.items[0..self.len]) |p| try p.stage(p.ptr, view, encoded_position);
    }

    /// Borrowed only for as long as `self` itself is: the returned
    /// `Participant` keeps a pointer back into `self.items`, exactly like
    /// every other `Participant` implementation in this tree keeps a
    /// pointer back into its own owning value.
    pub fn participant(self: *List) Participant {
        return .{ .ptr = self, .reset = reset, .observe = observe, .stage = stage };
    }
};

const std = @import("std");

const TestTxn = struct {
    values: std.StringHashMapUnmanaged([]const u8) = .empty,

    pub fn get(self: *TestTxn, key: []const u8) ![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }

    pub fn put(self: *TestTxn, key: []const u8, value: []const u8) !void {
        try self.values.put(std.testing.allocator, key, value);
    }

    pub fn deinit(self: *TestTxn) void {
        self.values.deinit(std.testing.allocator);
    }
};

const RecordingParticipant = struct {
    stage_calls: usize = 0,
    reset_calls: usize = 0,
    key_to_write: []const u8,
    value_to_write: []const u8,

    fn reset(ptr: *anyopaque) void {
        const self: *RecordingParticipant = @ptrCast(@alignCast(ptr));
        self.reset_calls += 1;
    }
    fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
    fn stage(ptr: *anyopaque, view: View, _: []const u8) anyerror!void {
        const self: *RecordingParticipant = @ptrCast(@alignCast(ptr));
        self.stage_calls += 1;
        try view.put(self.key_to_write, self.value_to_write);
    }

    fn participant(self: *RecordingParticipant) Participant {
        return .{ .ptr = self, .reset = reset, .observe = observe, .stage = stage };
    }
};

const FailingParticipant = struct {
    fn reset(_: *anyopaque) void {}
    fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
    fn stage(_: *anyopaque, _: View, _: []const u8) anyerror!void {
        return error.InjectedParticipantFailure;
    }
    fn participant(self: *FailingParticipant) Participant {
        return .{ .ptr = self, .reset = reset, .observe = observe, .stage = stage };
    }
};

test "commit participant list runs every member against the same view, in order" {
    var txn = TestTxn{};
    defer txn.deinit();

    var first = RecordingParticipant{ .key_to_write = "\x00\x00first", .value_to_write = "a" };
    var second = RecordingParticipant{ .key_to_write = "\x00\x00second", .value_to_write = "b" };

    var list = List{};
    try list.append(first.participant());
    try list.append(second.participant());
    const composite = list.participant();

    composite.reset(composite.ptr);
    try composite.stage(composite.ptr, View.from(&txn), "encoded-position");

    try std.testing.expectEqual(@as(usize, 1), first.reset_calls);
    try std.testing.expectEqual(@as(usize, 1), first.stage_calls);
    try std.testing.expectEqual(@as(usize, 1), second.stage_calls);
    try std.testing.expectEqualStrings("a", try txn.get("\x00\x00first"));
    try std.testing.expectEqualStrings("b", try txn.get("\x00\x00second"));
}

test "commit participant list aborts on the first member's failure without running the rest" {
    var txn = TestTxn{};
    defer txn.deinit();

    var failing = FailingParticipant{};
    var second = RecordingParticipant{ .key_to_write = "\x00\x00second", .value_to_write = "b" };

    var list = List{};
    try list.append(failing.participant());
    try list.append(second.participant());
    const composite = list.participant();

    try std.testing.expectError(error.InjectedParticipantFailure, composite.stage(composite.ptr, View.from(&txn), ""));
    try std.testing.expectEqual(@as(usize, 0), second.stage_calls);
}

test "commit participant list rejects a fifth member past its fixed capacity" {
    var list = List{};
    var p = RecordingParticipant{ .key_to_write = "\x00\x00k", .value_to_write = "v" };
    for (0..List.max_participants) |_| try list.append(p.participant());
    try std.testing.expectError(error.TooManyCommitParticipants, list.append(p.participant()));
}

test "view putIndexCatalog reports unsupported when the host transaction has no second catalog root" {
    var txn = TestTxn{};
    defer txn.deinit();

    const view = View.from(&txn);
    try std.testing.expectError(error.IndexCatalogUnsupported, view.putIndexCatalog("scope", &.{}));
}

const IndexCatalogTxn = struct {
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
    last_scope: []const u8 = "",

    pub fn get(self: *IndexCatalogTxn, key: []const u8) ![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }
    pub fn put(self: *IndexCatalogTxn, key: []const u8, value: []const u8) !void {
        try self.values.put(std.testing.allocator, key, value);
    }
    pub fn putIndexCatalogBatch(self: *IndexCatalogTxn, scope: []const u8, mutations: []const IndexCatalogMutation) !void {
        self.last_scope = scope;
        for (mutations) |m| try self.values.put(std.testing.allocator, m.key, m.value);
    }
    pub fn deinit(self: *IndexCatalogTxn) void {
        self.values.deinit(std.testing.allocator);
    }
};

test "view putIndexCatalog reaches a host transaction that implements putIndexCatalogBatch" {
    var txn = IndexCatalogTxn{};
    defer txn.deinit();

    const view = View.from(&txn);
    try view.putIndexCatalog("idx-scope", &.{
        .{ .namespace = "segments", .key = "seg:1", .value = "bytes" },
    });
    try std.testing.expectEqualStrings("idx-scope", txn.last_scope);
    try std.testing.expectEqualStrings("bytes", try txn.get("seg:1"));
}
