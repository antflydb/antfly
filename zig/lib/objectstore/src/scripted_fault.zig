// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Backend-neutral deterministic fault wrapper for object-store protocols.
//! The wrapper is deliberately independent of Antfly and VOPR: a scheduler,
//! fuzz test, or ordinary unit test programs the next operation, while the
//! production object-store client contract remains unchanged.

const std = @import("std");
const client_mod = @import("client.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const Client = struct {
    allocator: Allocator,
    backing: client_mod.Client,
    next_put: PutFault = .none,
    next_get_error: ?anyerror = null,
    hidden_bucket: ?[]u8 = null,
    hidden_key: ?[]u8 = null,
    hidden_reads_remaining: usize = 0,
    put_attempts: u64 = 0,
    get_attempts: u64 = 0,

    pub const PutFault = union(enum) {
        none,
        fail_before: anyerror,
        commit_then_fail: anyerror,
        partial_then_fail: struct {
            bytes: usize,
            err: anyerror,
        },
        duplicate,
        hide_after_success: usize,
    };

    pub fn init(allocator: Allocator, backing: client_mod.Client) Client {
        return .{ .allocator = allocator, .backing = backing };
    }

    pub fn deinit(self: *Client) void {
        self.clearHidden();
        self.* = undefined;
    }

    pub fn client(self: *Client) client_mod.Client {
        return .{ .allocator = self.allocator, .ptr = self, .vtable = &vtable };
    }

    pub fn failNextPutBefore(self: *Client, err: anyerror) void {
        self.next_put = .{ .fail_before = err };
    }

    pub fn commitNextPutThenFail(self: *Client, err: anyerror) void {
        self.next_put = .{ .commit_then_fail = err };
    }

    pub fn partiallyCommitNextPut(self: *Client, bytes: usize, err: anyerror) void {
        self.next_put = .{ .partial_then_fail = .{ .bytes = bytes, .err = err } };
    }

    pub fn duplicateNextPut(self: *Client) void {
        self.next_put = .duplicate;
    }

    pub fn hideNextSuccessfulPut(self: *Client, reads: usize) void {
        self.next_put = .{ .hide_after_success = reads };
    }

    pub fn failNextGet(self: *Client, err: anyerror) void {
        self.next_get_error = err;
    }

    /// Models a client/process crash: pending call-local faults disappear,
    /// while committed backing data and provider-side visibility state remain.
    pub fn resetClientAfterCrash(self: *Client) void {
        self.next_put = .none;
        self.next_get_error = null;
    }

    fn takePutFault(self: *Client) PutFault {
        const fault = self.next_put;
        self.next_put = .none;
        return fault;
    }

    fn rememberHidden(self: *Client, bucket: []const u8, key: []const u8, reads: usize) !void {
        self.clearHidden();
        if (reads == 0) return;
        self.hidden_bucket = try self.allocator.dupe(u8, bucket);
        errdefer {
            self.allocator.free(self.hidden_bucket.?);
            self.hidden_bucket = null;
        }
        self.hidden_key = try self.allocator.dupe(u8, key);
        self.hidden_reads_remaining = reads;
    }

    fn clearHidden(self: *Client) void {
        if (self.hidden_bucket) |bucket| self.allocator.free(bucket);
        if (self.hidden_key) |key| self.allocator.free(key);
        self.hidden_bucket = null;
        self.hidden_key = null;
        self.hidden_reads_remaining = 0;
    }

    fn consumeHiddenRead(self: *Client, bucket: []const u8, key: []const u8) bool {
        if (self.hidden_reads_remaining == 0) return false;
        if (!std.mem.eql(u8, self.hidden_bucket.?, bucket) or !std.mem.eql(u8, self.hidden_key.?, key)) return false;
        self.hidden_reads_remaining -= 1;
        if (self.hidden_reads_remaining == 0) self.clearHidden();
        return true;
    }

    fn erasedDeinit(_: Allocator, _: *anyopaque) void {}

    fn bucketExists(ptr: *anyopaque, bucket: []const u8) !bool {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.backing.vtable.bucket_exists(self.backing.ptr, bucket);
    }

    fn makeBucket(ptr: *anyopaque, bucket: []const u8) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        try self.backing.vtable.make_bucket(self.backing.ptr, bucket);
    }

    fn putObject(
        ptr: *anyopaque,
        alloc: Allocator,
        bucket: []const u8,
        key: []const u8,
        body: []const u8,
        options: types.PutOptions,
    ) !types.PutResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        self.put_attempts +|= 1;
        return switch (self.takePutFault()) {
            .none => self.backing.vtable.put_object(self.backing.ptr, alloc, bucket, key, body, options),
            .fail_before => |err| err,
            .commit_then_fail => |err| blk: {
                var result = try self.backing.vtable.put_object(self.backing.ptr, alloc, bucket, key, body, options);
                result.deinit(alloc);
                break :blk err;
            },
            .partial_then_fail => |fault| blk: {
                var result = try self.backing.vtable.put_object(
                    self.backing.ptr,
                    alloc,
                    bucket,
                    key,
                    body[0..@min(fault.bytes, body.len)],
                    options,
                );
                result.deinit(alloc);
                break :blk fault.err;
            },
            .duplicate => blk: {
                var first = try self.backing.vtable.put_object(self.backing.ptr, alloc, bucket, key, body, options);
                first.deinit(alloc);
                break :blk self.backing.vtable.put_object(self.backing.ptr, alloc, bucket, key, body, options);
            },
            .hide_after_success => |reads| blk: {
                const result = try self.backing.vtable.put_object(self.backing.ptr, alloc, bucket, key, body, options);
                self.rememberHidden(bucket, key, reads) catch {
                    var owned = result;
                    owned.deinit(alloc);
                    return error.SystemResources;
                };
                break :blk result;
            },
        };
    }

    fn getObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: types.GetOptions) !types.GetResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        self.get_attempts +|= 1;
        if (self.next_get_error) |err| {
            self.next_get_error = null;
            return err;
        }
        if (self.consumeHiddenRead(bucket, key)) return error.FileNotFound;
        return self.backing.vtable.get_object(self.backing.ptr, alloc, bucket, key, options);
    }

    fn getObjectAttributes(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectAttributes {
        const self: *Client = @ptrCast(@alignCast(ptr));
        if (self.consumeHiddenRead(bucket, key)) return error.FileNotFound;
        return self.backing.vtable.get_object_attributes(self.backing.ptr, alloc, bucket, key);
    }

    fn statObject(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !types.ObjectMetadata {
        const self: *Client = @ptrCast(@alignCast(ptr));
        if (self.consumeHiddenRead(bucket, key)) return error.FileNotFound;
        return self.backing.vtable.stat_object(self.backing.ptr, alloc, bucket, key);
    }

    fn deleteObject(ptr: *anyopaque, bucket: []const u8, key: []const u8, options: types.DeleteOptions) !void {
        const self: *Client = @ptrCast(@alignCast(ptr));
        try self.backing.vtable.delete_object(self.backing.ptr, bucket, key, options);
    }

    fn listObjects(ptr: *anyopaque, alloc: Allocator, bucket: []const u8, options: types.ListOptions) !types.ListResult {
        const self: *Client = @ptrCast(@alignCast(ptr));
        return self.backing.vtable.list_objects(self.backing.ptr, alloc, bucket, options);
    }

    const vtable: client_mod.Client.VTable = .{
        .deinit = erasedDeinit,
        .bucket_exists = bucketExists,
        .make_bucket = makeBucket,
        .put_object = putObject,
        .get_object = getObject,
        .get_object_attributes = getObjectAttributes,
        .stat_object = statObject,
        .delete_object = deleteObject,
        .list_objects = listObjects,
    };
};

test "scripted object store faults preserve committed state across retry and crash" {
    const objectstore = @import("root.zig");
    var memory = objectstore.MemoryClient.init(std.testing.allocator);
    defer memory.deinit();
    var faults = Client.init(std.testing.allocator, memory.client());
    defer faults.deinit();
    var client = faults.client();
    try client.makeBucket("bucket");

    faults.partiallyCommitNextPut(2, error.Timeout);
    try std.testing.expectError(error.Timeout, client.putObject("bucket", "key", "complete", .{}));
    var partial = try client.getObject("bucket", "key", .{});
    defer partial.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("co", partial.body);

    faults.resetClientAfterCrash();
    var put = try client.putObject("bucket", "key", "complete", .{});
    put.deinit(std.testing.allocator);
    faults.hideNextSuccessfulPut(2);
    var hidden_put = try client.putObject("bucket", "visible-later", "value", .{});
    hidden_put.deinit(std.testing.allocator);
    try std.testing.expectError(error.FileNotFound, client.getObject("bucket", "visible-later", .{}));
    try std.testing.expectError(error.FileNotFound, client.getObject("bucket", "visible-later", .{}));
    var visible = try client.getObject("bucket", "visible-later", .{});
    defer visible.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("value", visible.body);
}
