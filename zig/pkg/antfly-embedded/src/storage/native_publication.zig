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

//! Borrowed, synchronous participation in an externally durable index WAL
//! publication. The host owns admission, rollback, durability and uncertainty.
//! A participant may stage operations, but cannot commit or activate memory.
const std = @import("std");
const abi = @import("../runtime_callback_abi.zig");

pub const Scope = struct {
    inode: u64,
    file_namespace: [32]u8 = @splat(0),
    prefix: []u8,
    pub fn deinit(self: *Scope, alloc: std.mem.Allocator) void {
        alloc.free(self.prefix);
        self.* = undefined;
    }
};

pub const View = struct {
    ptr: *anyopaque,
    link_fn: *const fn (*anyopaque, u64, []const u8, []const u8, usize) anyerror!void,

    put_fn: *const fn (*anyopaque, u64, []const u8, []const u8) anyerror!void,
    get_metadata_fn: *const fn (*anyopaque, std.mem.Allocator, Scope, []const u8) anyerror![]u8,
    put_metadata_fn: *const fn (*anyopaque, Scope, []const u8, []const u8) anyerror!void,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = abi.Boundary(struct {
        link: *const fn (*anyopaque, u64, []const u8, []const u8, usize) anyerror!void,
        put: *const fn (*anyopaque, u64, []const u8, []const u8) anyerror!void,
        get_metadata: *const fn (*anyopaque, std.mem.Allocator, Scope, []const u8) anyerror![]u8,
        put_metadata: *const fn (*anyopaque, Scope, []const u8, []const u8) anyerror!void,
    });

    pub fn getMetadata(self: View, alloc: std.mem.Allocator, scope: Scope, key: []const u8) ![]u8 {
        return Boundary.call("get_metadata", self.dispatch, self.get_metadata_fn, .{ self.ptr, alloc, scope, key });
    }
    pub fn putMetadata(self: View, scope: Scope, key: []const u8, value: []const u8) !void {
        return Boundary.call("put_metadata", self.dispatch, self.put_metadata_fn, .{ self.ptr, scope, key, value });
    }

    pub fn put(self: View, inode: u64, path: []const u8, bytes: []const u8) !void {
        return Boundary.call("put", self.dispatch, self.put_fn, .{ self.ptr, inode, path, bytes });
    }

    pub fn link(self: View, inode: u64, construction: []const u8, path: []const u8, length: usize) !void {
        return Boundary.call("link", self.dispatch, self.link_fn, .{ self.ptr, inode, construction, path, length });
    }
};

pub const Participant = struct {
    ptr: *anyopaque,
    stage: *const fn (*anyopaque, View) anyerror!void,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    const Boundary = abi.Boundary(struct { stage: *const fn (*anyopaque, View) anyerror!void });
    pub fn apply(self: Participant, view: View) !void {
        return Boundary.call("stage", self.dispatch, self.stage, .{ self.ptr, view });
    }
};

/// Restricted transaction adapter for projection progress and revocation rules.
/// Returned slices remain owned until this synchronous participant returns.
pub const MetadataTxn = struct {
    alloc: std.mem.Allocator,
    view: View,
    scope: Scope,
    reads: std.ArrayListUnmanaged([]u8) = .empty,
    pub fn deinit(self: *MetadataTxn) void {
        for (self.reads.items) |raw| self.alloc.free(raw);
        self.reads.deinit(self.alloc);
    }
    pub fn get(self: *MetadataTxn, key: []const u8) ![]const u8 {
        const raw = try self.view.getMetadata(self.alloc, self.scope, key);
        errdefer self.alloc.free(raw);
        try self.reads.append(self.alloc, raw);
        return raw;
    }
    pub fn put(self: *MetadataTxn, key: []const u8, value: []const u8) !void {
        try self.view.putMetadata(self.scope, key, value);
    }
};
