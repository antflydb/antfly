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

//! Shared crash-safe publication primitives for immutable generations.
//!
//! AtomicWriteSink can report an error after rename when the parent-directory
//! fsync fails. At that point the destination is visible but its crash
//! durability is uncertain. Immutable data files are safe to retry at their
//! deterministic paths. Mutable control files (CURRENT/AUTHORITY) are retried
//! once with the identical bytes, which both resolves the visible namespace
//! and repeats the directory durability barrier. A second failure is an
//! explicit poison boundary: callers must reopen before any later write.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const lsm_backend = @import("lsm_backend/mod.zig");

var test_post_publish_failures: std.atomic.Value(u32) = .init(0);

pub fn replaceImmutable(
    alloc: Allocator,
    storage: lsm_backend.Storage,
    path: []const u8,
    contents: []const u8,
) !void {
    var sink = try storage.beginAtomicWrite(alloc, path);
    var active = true;
    defer if (active) sink.abort();
    try sink.appendSlice(contents);
    active = false;
    try sink.finish();
}

/// Stages a one-pass immutable data file without displacing the serving
/// generation from the page cache. WAL and authority replacements use the
/// normal primitive above because their new bytes may be consumed or appended
/// immediately after publication.
pub fn replaceColdImmutable(
    alloc: Allocator,
    storage: lsm_backend.Storage,
    path: []const u8,
    contents: []const u8,
) !void {
    var sink = try storage.beginAtomicWrite(alloc, path);
    var active = true;
    defer if (active) sink.abort();
    // Immutable generation data is produced by one-pass maintenance and is
    // normally not read until after CURRENT publication.  Keeping the output
    // resident competes with the active serving generation and can double RSS
    // during a large checkpoint or compaction.
    sink.setCacheIntent(.cold_sequential);
    try sink.appendSlice(contents);
    active = false;
    try sink.finish();
}

const ControlAttempt = union(enum) {
    success,
    definitive_error: anyerror,
    ambiguous_error: anyerror,
};

fn publishControlAttempt(
    alloc: Allocator,
    storage: lsm_backend.Storage,
    path: []const u8,
    contents: []const u8,
) ControlAttempt {
    var sink = storage.beginAtomicWrite(alloc, path) catch |err| return .{ .definitive_error = err };
    var active = true;
    defer if (active) sink.abort();
    sink.appendSlice(contents) catch |err| return .{ .definitive_error = err };
    active = false;
    sink.finish() catch |err| return .{ .ambiguous_error = err };
    if (consumeTestPostPublishFailure()) return .{ .ambiguous_error = error.InjectedPostPublishFailure };
    return .success;
}

/// Publishes a small generation authority record. Success means the exact
/// bytes passed the atomic rename and parent-directory durability boundary.
/// GenerationPublicationDurabilityUncertain means the destination may already
/// contain those bytes and the owning store must be poisoned and reopened.
pub fn publishControlFile(
    alloc: Allocator,
    storage: lsm_backend.Storage,
    path: []const u8,
    contents: []const u8,
) !void {
    const first = publishControlAttempt(alloc, storage, path, contents);
    switch (first) {
        .success => return,
        .definitive_error => |err| return err,
        .ambiguous_error => |first_err| switch (publishControlAttempt(alloc, storage, path, contents)) {
            .success => return,
            .definitive_error, .ambiguous_error => |retry_err| {
                std.log.err("generation control publication durability uncertain path={s} first_err={s} retry_err={s}", .{
                    path,
                    @errorName(first_err),
                    @errorName(retry_err),
                });
                return error.GenerationPublicationDurabilityUncertain;
            },
        },
    }
}

fn consumeTestPostPublishFailure() bool {
    if (!builtin.is_test) return false;
    var remaining = test_post_publish_failures.load(.acquire);
    while (remaining != 0) {
        if (test_post_publish_failures.cmpxchgWeak(
            remaining,
            remaining - 1,
            .acq_rel,
            .acquire,
        )) |actual| {
            remaining = actual;
        } else return true;
    }
    return false;
}

pub fn injectPostPublishFailuresForTest(count: u32) void {
    if (!builtin.is_test) return;
    test_post_publish_failures.store(count, .release);
}

test "control publication resolves one ambiguous post-rename failure" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    try memory.storage().createDirPath("/generation-publication");

    injectPostPublishFailuresForTest(1);
    try publishControlFile(alloc, memory.storage(), "/generation-publication/CURRENT", "generation-two");
    const current = try memory.storage().readFileAlloc(alloc, "/generation-publication/CURRENT", 64);
    defer alloc.free(current);
    try std.testing.expectEqualStrings("generation-two", current);
}

test "control publication fails closed after repeated ambiguous failures" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    try memory.storage().createDirPath("/generation-publication-poison");

    injectPostPublishFailuresForTest(2);
    try std.testing.expectError(
        error.GenerationPublicationDurabilityUncertain,
        publishControlFile(alloc, memory.storage(), "/generation-publication-poison/CURRENT", "generation-two"),
    );
    // The error deliberately does not claim that rename failed. A caller can
    // observe the new record, but may not continue until a reopen revalidates
    // the complete generation and establishes a later durability barrier.
    const current = try memory.storage().readFileAlloc(alloc, "/generation-publication-poison/CURRENT", 64);
    defer alloc.free(current);
    try std.testing.expectEqualStrings("generation-two", current);
}
