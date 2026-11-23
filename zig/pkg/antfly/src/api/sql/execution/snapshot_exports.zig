// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Process-local names for genuine retained cuts. Names grant no row authority.
//! The exporter owns discovery until its next command/disconnect; importers own
//! independent references thereafter. Restart never reconstructs a newer cut.
const std = @import("std");
const retained = @import("retained_snapshot.zig");
const Realm = @import("antfly_local_sources").system_catalog_logical_replication_publisher.Realm;
const A = std.mem.Allocator;
pub const Token = [16]u8;
pub const Owner = [32]u8;
pub const Directory = struct {
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.AutoHashMapUnmanaged(Token, Entry) = .empty,
    owners: std.AutoHashMapUnmanaged(Owner, Token) = .empty,
    pub const max_exports = 4096;
    const Entry = struct { owner: Owner, realm: Realm, cut: *retained.Shared, ready: bool = false };
    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Caller supplies cryptographic entropy and already owns the real cut.
    /// Reserve every allocation BEFORE proposing creation of the durable slot.
    pub fn reserve(self: *@This(), a: A, owner: Owner, token: Token, realm: Realm, cut: *retained.Shared) !void {
        try realm.validate();
        if (std.mem.allEqual(u8, &owner, 0) or std.mem.allEqual(u8, &token, 0)) return error.InvalidLogicalPublisher;
        self.lock();
        defer self.mutex.unlock();
        if (self.owners.contains(owner) or self.entries.contains(token)) return error.SqlSnapshotCaptureChanged;
        if (self.entries.count() == max_exports) return error.SqlProgramLimitExceeded;
        try self.entries.ensureUnusedCapacity(a, 1);
        try self.owners.ensureUnusedCapacity(a, 1);
        self.entries.putAssumeCapacity(token, .{ .owner = owner, .realm = realm, .cut = cut.retain() });
        self.owners.putAssumeCapacity(owner, token);
    }
    /// Infallible publication follows the accepting native slot decision.
    pub fn publish(self: *@This(), owner: Owner, token: Token) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(std.mem.eql(u8, &self.owners.get(owner).?, &token));
        const entry = self.entries.getPtr(token).?;
        std.debug.assert(std.mem.eql(u8, &entry.owner, &owner) and !entry.ready);
        entry.ready = true;
    }
    pub fn acquire(self: *@This(), token: Token, realm: Realm) !*retained.Shared {
        self.lock();
        defer self.mutex.unlock();
        const entry = self.entries.get(token) orelse return error.SqlSnapshotNotFound;
        if (!entry.ready) return error.SqlSnapshotNotFound;
        if (!entry.realm.eql(realm)) return error.SqlSnapshotDatabaseMismatch;
        if (entry.cut.lost.load(.acquire)) return error.RetainedReadRestartRequired;
        return entry.cut.retain();
    }
    pub fn ownerCut(self: *@This(), owner: Owner) ?*retained.Shared {
        self.lock();
        defer self.mutex.unlock();
        const token = self.owners.get(owner) orelse return null;
        const entry = self.entries.get(token).?;
        return if (entry.ready) entry.cut.retain() else null;
    }
    pub fn retire(self: *@This(), owner: Owner) void {
        self.lock();
        defer self.mutex.unlock();
        const token = self.owners.fetchRemove(owner) orelse return;
        const entry = self.entries.fetchRemove(token.value).?;
        // release only enqueues retirement. No owner I/O runs under the map lock.
        entry.value.cut.release();
    }
    pub fn deinit(self: *@This(), a: A) void {
        var entries = self.entries.valueIterator();
        while (entries.next()) |entry| entry.cut.release();
        self.entries.deinit(a);
        self.owners.deinit(a);
        self.* = .{};
    }
};
pub fn format(token: Token) [39]u8 {
    return "antfly-".* ++ std.fmt.bytesToHex(token, .lower);
}
pub fn parse(text: []const u8) !Token {
    if (text.len != 39 or !std.mem.startsWith(u8, text, "antfly-")) return error.SqlInvalidSnapshotIdentifier;
    var token: Token = undefined;
    _ = std.fmt.hexToBytes(&token, text[7..]) catch return error.SqlInvalidSnapshotIdentifier;
    if (std.mem.allEqual(u8, &token, 0)) return error.SqlInvalidSnapshotIdentifier;
    return token;
}

const Fixture = struct {
    closes: usize = 0,
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.closes += 1;
    }
    fn open(_: *anyopaque, _: A, _: @import("antfly_local_sources").api_table_read_source.RelationalStatementScan) !@import("antfly_local_sources").api_table_read_source.RelationalReadView {
        return error.TestUnexpectedResult;
    }
    fn cut(self: *@This(), a: A, retirement: *retained.Retirement) !*retained.Shared {
        return retained.Shared.create(a, .{ .ptr = self, .vtable = &.{ .open = open, .close = close } }, retirement);
    }
    const realm: Realm = .{ .group_id = 7, .database_id = 11, .incarnation = @splat('1') };
};
fn allocationCampaign(a: A) !void {
    var fixture: Fixture = .{};
    var retirement: retained.Retirement = .{};
    defer retirement.drain();
    const cut = try fixture.cut(a, &retirement);
    defer cut.release();
    var directory: Directory = .{};
    defer directory.deinit(a);
    try directory.reserve(a, @splat(2), @splat(3), Fixture.realm, cut);
    try std.testing.expectError(error.SqlSnapshotNotFound, directory.acquire(@splat(3), Fixture.realm));
    directory.publish(@splat(2), @splat(3));
    const imported = try directory.acquire(@splat(3), Fixture.realm);
    defer imported.release();
    directory.retire(@splat(2));
    try std.testing.expectError(error.SqlSnapshotNotFound, directory.acquire(@splat(3), Fixture.realm));
    directory.retire(@splat(2));
    try std.testing.expectEqual(0, fixture.closes);
}
test "SQL retained snapshot exports preserve importer lifetime and reservation atomicity through every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationCampaign, .{});
}
test "SQL retained snapshot exports enforce realm birth unpublished and lost-cut fences without closing under locks" {
    const a = std.testing.allocator;
    var fixture: Fixture = .{};
    var retirement: retained.Retirement = .{};
    defer retirement.drain();
    const cut = try fixture.cut(a, &retirement);
    var directory: Directory = .{};
    defer directory.deinit(a);
    try directory.reserve(a, @splat(2), @splat(3), Fixture.realm, cut);
    directory.publish(@splat(2), @splat(3));
    const imported = try directory.acquire(@splat(3), Fixture.realm);
    try std.testing.expect(imported == cut);
    var foreign = Fixture.realm;
    foreign.database_id += 1;
    try std.testing.expectError(error.SqlSnapshotDatabaseMismatch, directory.acquire(@splat(3), foreign));
    foreign = Fixture.realm;
    foreign.incarnation = @splat('2');
    try std.testing.expectError(error.SqlSnapshotDatabaseMismatch, directory.acquire(@splat(3), foreign));
    cut.lost.store(true, .release);
    try std.testing.expectError(error.RetainedReadRestartRequired, directory.acquire(@splat(3), Fixture.realm));
    directory.retire(@splat(2));
    cut.release();
    retirement.drain();
    try std.testing.expectEqual(0, fixture.closes);
    imported.release();
    retirement.drain();
    try std.testing.expectEqual(1, fixture.closes);
    const token: Token = @splat(7);
    try std.testing.expectEqual(token, try parse(&format(token)));
    for ([_][]const u8{ "", "../snapshot", "antfly-00000000000000000000000000000000", "antfly-xyz" }) |bad| try std.testing.expectError(error.SqlInvalidSnapshotIdentifier, parse(bad));
}
