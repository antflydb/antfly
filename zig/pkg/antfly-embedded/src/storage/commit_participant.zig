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

/// Key kinds a commit participant may write/delete through `View.putReplay`/
/// `View.deleteReplay` -- derived-coverage outcome markers/counters
/// (`setDerivedCoverageOutcomes`) and embedding-artifact repair-issue
/// records (`clearPublishedArtifactRepairIssueContext`). Every one of these
/// keys is `[internal_keys.replay_namespace, 0xff, <kind>]`-prefixed
/// (`internal_keys.globalReplayPrefix`); allow-listing by kind byte keeps
/// this an escape hatch for exactly those two mechanisms' own keys, not an
/// unrestricted write into the replay namespace generally.
const internal_keys = @import("internal_keys.zig");
const allowed_replay_kinds = [_]u8{
    internal_keys.derived_coverage_kind,
    internal_keys.artifact_repair_issue_kind,
    internal_keys.artifact_repair_kind_issue_kind,
    internal_keys.artifact_repair_completion_kind,
};

fn isAllowedReplayKey(key: []const u8) bool {
    if (key.len < 3 or key[0] != internal_keys.replay_namespace or key[1] != 0xff) return false;
    for (allowed_replay_kinds) |kind| {
        if (key[2] == kind) return true;
    }
    return false;
}

/// `algebraic/index.zig`'s entire keyspace lives under the private
/// `\x00\x00__algebraic__:` namespace (see `storagePrefixAlloc`), but its
/// keys are built by `algebraic/token.zig`'s `canonicalTupleAlloc`, which
/// length-prefixes every tuple component -- including that leading
/// `\x00\x00` sentinel -- as `"<len>:<component>"` rather than placing the
/// sentinel at the raw key's first two bytes. A literal byte check (`put`'s
/// `key[0] != 0 or key[1] != 0`) therefore rejects every algebraic write
/// staged through this restricted view (WS3.2b's inline algebraic
/// publication, `mutation_materialization.zig`'s `InlineAlgebraicPublication`)
/// even though its whole keyspace is exactly as private as a literal
/// `\x00\x00`-prefixed key. Recognize the encoded form directly instead of
/// importing `algebraic/token.zig` here (this file stays index-encoding
/// agnostic): skip the tuple's own `<len>:` header and check whether the
/// first `len` payload bytes themselves start with the sentinel.
fn isTupleEncodedPrivateKey(key: []const u8) bool {
    const colon = std.mem.indexOfScalar(u8, key, ':') orelse return false;
    if (colon == 0) return false;
    for (key[0..colon]) |c| {
        if (!std.ascii.isDigit(c)) return false;
    }
    const component_len = std.fmt.parseInt(usize, key[0..colon], 10) catch return false;
    const payload_start = colon + 1;
    if (component_len < 2 or payload_start + component_len > key.len) return false;
    return key[payload_start] == 0 and key[payload_start + 1] == 0;
}

/// Private metadata, in either of the two shapes this restricted commit
/// view's producers actually write: a literal `\x00\x00`-prefixed key (every
/// non-algebraic private key in this tree), or `algebraic/index.zig`'s
/// tuple-encoded equivalent (see `isTupleEncodedPrivateKey`).
fn isPrivateMetadataKey(key: []const u8) bool {
    if (key.len >= 2 and key[0] == 0 and key[1] == 0) return true;
    return isTupleEncodedPrivateKey(key);
}

/// Read-only cursor type returned by `View.openCursor`. `backend_erased.zig`
/// is itself backend-agnostic (a type-erased vtable, not a Lite-specific
/// module), so referencing its `Cursor` here keeps this file's "never import
/// a Lite-specific module" property intact.
pub const Cursor = @import("backend_erased.zig").Cursor;

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
    /// WS4 phase 2 (coverage/repair-issue ride-along) and algebraic inline
    /// apply (see `mutation_materialization.zig`'s
    /// `InlineAlgebraicPublication`): optional delete capability into the
    /// private metadata namespace, wired whenever the host transaction
    /// supports it. Algebraic index storage already lives entirely under
    /// that namespace (`\x00\x00__algebraic__:` and sibling prefixes in
    /// `storage/db/algebraic/index.zig`), so its read-modify-write fact
    /// maintenance needs `delete`/`openCursor` on the SAME restricted
    /// surface as `put` above, not a second catalog root. `null` on a host
    /// with no such method (every backend `View.from` sees today supports
    /// it, but `putReplay`/`deleteReplay`/algebraic fall back safely via
    /// `error.DeleteUnsupported`/`error.CursorUnsupported`).
    delete_fn: ?*const fn (*anyopaque, []const u8) anyerror!void = null,
    open_cursor_fn: ?*const fn (*anyopaque) anyerror!Cursor = null,
    append_put_fn: ?*const fn (*anyopaque, []const u8, []const u8) anyerror!void = null,
    get_many_sorted_fn: ?*const fn (*anyopaque, []const []const u8, []?[]const u8) anyerror!void = null,

    pub fn get(self: View, key: []const u8) ![]const u8 {
        return self.get_fn(self.ptr, key);
    }

    pub fn put(self: View, key: []const u8, value: []const u8) !void {
        // Public rows and derived artifacts must not change after their
        // revision stamps were finalized. Only the private metadata namespace
        // is writable through this restricted commit view.
        if (!isPrivateMetadataKey(key)) return error.InvalidCommitMetadata;
        return self.put_fn(self.ptr, key, value);
    }

    /// Same private-metadata-only restriction as `put`, for the same reason.
    pub fn delete(self: View, key: []const u8) !void {
        if (!isPrivateMetadataKey(key)) return error.InvalidCommitMetadata;
        const delete_fn = self.delete_fn orelse return error.DeleteUnsupported;
        return delete_fn(self.ptr, key);
    }

    /// Read-only: ranges over the host transaction's full keyspace (not
    /// restricted to the private metadata namespace), exactly like `get`
    /// above. `error.CursorUnsupported` when the host transaction does not
    /// expose one.
    pub fn openCursor(self: View) !Cursor {
        const open_cursor_fn = self.open_cursor_fn orelse return error.CursorUnsupported;
        return open_cursor_fn(self.ptr);
    }

    /// Same private-metadata-only restriction as `put`; same bulk-append
    /// shortcut the host transaction's own `appendPut` provides.
    /// `error.AppendPutUnsupported` when the host transaction does not
    /// expose one (a participant may always fall back to `put`).
    pub fn appendPut(self: View, key: []const u8, value: []const u8) !void {
        if (!isPrivateMetadataKey(key)) return error.InvalidCommitMetadata;
        const append_put_fn = self.append_put_fn orelse return error.AppendPutUnsupported;
        return append_put_fn(self.ptr, key, value);
    }

    /// Read-only, unrestricted keyspace, like `get`. `error.GetManySortedUnsupported`
    /// when the host transaction does not expose one.
    pub fn getManySorted(self: View, keys: []const []const u8, values: []?[]const u8) !void {
        const get_many_sorted_fn = self.get_many_sorted_fn orelse return error.GetManySortedUnsupported;
        return get_many_sorted_fn(self.ptr, keys, values);
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

    /// Writes one derived-coverage or repair-issue record (see
    /// `allowed_replay_kinds`) into the primary keyspace, in the SAME
    /// native transaction as the primary write. These keys are
    /// `replay_namespace`-prefixed, not `\x00\x00`-prefixed, so `put`
    /// itself would reject them; this is the narrow, allow-listed surface
    /// `InlineDensePublication`/`InlineSparsePublication` use to keep
    /// derived-coverage accounting and repair-issue clearing consistent
    /// with the slow path without paying for a second durable commit.
    pub fn putReplay(self: View, key: []const u8, value: []const u8) !void {
        if (!isAllowedReplayKey(key)) return error.InvalidCommitMetadata;
        return self.put_fn(self.ptr, key, value);
    }

    /// Deletes one derived-coverage or repair-issue record (see `putReplay`).
    /// `error.ReplayDeleteUnsupported` when the host transaction has no
    /// delete capability wired (see `delete_fn`).
    pub fn deleteReplay(self: View, key: []const u8) !void {
        if (!isAllowedReplayKey(key)) return error.InvalidCommitMetadata;
        const delete_fn = self.delete_fn orelse return error.ReplayDeleteUnsupported;
        return delete_fn(self.ptr, key);
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
            fn delete(ptr: *anyopaque, key: []const u8) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.delete(key);
            }
            fn openCursor(ptr: *anyopaque) !Cursor {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.openCursor();
            }
            fn appendPut(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.appendPut(key, value);
            }
            fn getManySorted(ptr: *anyopaque, keys: []const []const u8, values: []?[]const u8) !void {
                const typed: T = @ptrCast(@alignCast(ptr));
                return typed.*.getManySorted(keys, values);
            }
        };
        return .{
            .ptr = @ptrCast(txn),
            .get_fn = Bridge.get,
            .put_fn = Bridge.put,
            .put_index_catalog_fn = if (@hasDecl(BaseType(T), "putIndexCatalogBatch")) Bridge.putIndexCatalog else null,
            .delete_fn = if (@hasDecl(BaseType(T), "delete")) Bridge.delete else null,
            .open_cursor_fn = if (@hasDecl(BaseType(T), "openCursor")) Bridge.openCursor else null,
            .append_put_fn = if (@hasDecl(BaseType(T), "appendPut")) Bridge.appendPut else null,
            .get_many_sorted_fn = if (@hasDecl(BaseType(T), "getManySorted")) Bridge.getManySorted else null,
        };
    }
};

pub const Participant = struct {
    ptr: *anyopaque,
    /// Fallible accounting covers physical mutations, including stamps staged
    /// by the host and private metadata staged by this participant.
    charge: ?*const fn (*anyopaque, []const u8, ?[]const u8) anyerror!void = null,
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
    /// WS4 phase 2 (text) started this at 4, sized for exactly one kind of
    /// contributor (authored_acceptance XOR one text publication) at a
    /// time. Dense, sparse and graph inline publications (lite-single-
    /// commit-write-path.md §3.3a) are each their OWN contributor appended
    /// alongside text's, so one write can now carry: authored_acceptance
    /// (1) + up to `mutation_commit.zig`'s own inline-text-publication cap
    /// (today 4, sized independently of this constant -- see that file's
    /// own array) + dense (1) + sparse (1) + graph (1) + headroom for
    /// algebraic indexes (§3.3b, not yet wired). Raised to 16 so that sum
    /// never silently saturates this fixed array; `append` still returns
    /// `error.TooManyCommitParticipants` (never a silent drop) if a future
    /// caller ever does exceed it, and every inline-publication call site
    /// that appends here must treat that error as "fall back to this
    /// contributor's own slow path", exactly like a `prepareInline*`
    /// failure, never as a reason to fail the whole write.
    pub const max_participants = 16;

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

    pub fn delete(self: *TestTxn, key: []const u8) !void {
        _ = self.values.remove(key);
    }

    pub fn deinit(self: *TestTxn) void {
        self.values.deinit(std.testing.allocator);
    }
};

test "view putReplay/deleteReplay allow only the derived-coverage and repair-issue key kinds" {
    var txn = TestTxn{};
    defer txn.deinit();
    const view = View.from(&txn);

    const coverage_key = &[_]u8{ internal_keys.replay_namespace, 0xff, internal_keys.derived_coverage_kind, 1 };
    try view.putReplay(coverage_key, "produced");
    try std.testing.expectEqualStrings("produced", try txn.get(coverage_key));

    const issue_key = &[_]u8{ internal_keys.replay_namespace, 0xff, internal_keys.artifact_repair_issue_kind, 2 };
    try view.putReplay(issue_key, "issue-bytes");
    try view.deleteReplay(issue_key);
    try std.testing.expectError(error.NotFound, txn.get(issue_key));

    // A replay-namespaced key OUTSIDE the allow-list (e.g. the generic
    // next-sequence counter) is rejected, same as `put`'s own `\x00\x00`
    // restriction rejects public keys.
    const disallowed_key = &[_]u8{ internal_keys.replay_namespace, 0xff, @backingInt(internal_keys.GlobalReplayKind.next_sequence), 3 };
    try std.testing.expectError(error.InvalidCommitMetadata, view.putReplay(disallowed_key, "x"));
    try std.testing.expectError(error.InvalidCommitMetadata, view.deleteReplay(disallowed_key));

    // `put`'s own `\x00\x00` surface still rejects a replay-namespaced key.
    try std.testing.expectError(error.InvalidCommitMetadata, view.put(coverage_key, "nope"));
}

test "view put/delete accept algebraic/index.zig's tuple-encoded private keys" {
    var txn = DeletingTxn{};
    defer txn.deinit();
    const view = View.from(&txn);

    // Shape produced by `algebraic/token.zig`'s `canonicalTupleAlloc` for
    // `algebraic/index.zig`'s `keyAlloc`: every component, including the
    // leading `\x00\x00__algebraic__` namespace sentinel, is wrapped as
    // `"<len>:<component>"` rather than placed at the raw key's first two
    // bytes. This is the exact key shape `InlineAlgebraicPublication`
    // (`mutation_materialization.zig`) stages through this restricted view.
    const algebraic_key = "15:\x00\x00__algebraic__|13:canonical/alg|2:v4|6:symbol|1:x";
    try view.put(algebraic_key, "sym-bytes");
    try std.testing.expectEqualStrings("sym-bytes", try txn.get(algebraic_key));
    try view.delete(algebraic_key);
    try std.testing.expectError(error.NotFound, txn.get(algebraic_key));
    try std.testing.expectError(error.AppendPutUnsupported, view.appendPut(algebraic_key, "x"));

    // A tuple-encoded key whose first component is NOT the private sentinel
    // (e.g. a public document row accidentally tuple-encoded) is still
    // rejected -- the exception is narrowly for the encoded private
    // namespace, not for every tuple-shaped key.
    const public_tuple_key = "5:doc:a|2:v1";
    try std.testing.expectError(error.InvalidCommitMetadata, view.put(public_tuple_key, "nope"));

    // A malformed length header (non-digit, or a length that overruns the
    // key) is rejected rather than panicking or misreading past the end.
    try std.testing.expectError(error.InvalidCommitMetadata, view.put("x:\x00\x00oops", "nope"));
    try std.testing.expectError(error.InvalidCommitMetadata, view.put("99:\x00\x00short", "nope"));
}

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

test "view delete and openCursor report unsupported when the host transaction has neither" {
    var txn = TestTxn{};
    defer txn.deinit();

    const view = View.from(&txn);
    try std.testing.expectError(error.DeleteUnsupported, view.delete("\x00\x00k"));
    try std.testing.expectError(error.CursorUnsupported, view.openCursor());
}

const DeletingTxn = struct {
    values: std.StringHashMapUnmanaged([]const u8) = .empty,

    pub fn get(self: *DeletingTxn, key: []const u8) ![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }
    pub fn put(self: *DeletingTxn, key: []const u8, value: []const u8) !void {
        try self.values.put(std.testing.allocator, key, value);
    }
    pub fn delete(self: *DeletingTxn, key: []const u8) !void {
        if (!self.values.remove(key)) return error.NotFound;
    }
    pub fn deinit(self: *DeletingTxn) void {
        self.values.deinit(std.testing.allocator);
    }
};

test "view delete reaches a host transaction that implements delete, restricted to private metadata keys" {
    var txn = DeletingTxn{};
    defer txn.deinit();

    const view = View.from(&txn);
    try view.put("\x00\x00k", "v");
    try std.testing.expectEqualStrings("v", try txn.get("\x00\x00k"));
    try view.delete("\x00\x00k");
    try std.testing.expectError(error.NotFound, txn.get("\x00\x00k"));
    try std.testing.expectError(error.InvalidCommitMetadata, view.delete("public-key"));
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
