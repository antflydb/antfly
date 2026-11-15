// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One immutable device operand inventory per loaded model and PJRT client.
//! Shape specializations borrow these buffers; eviction never uploads a second
//! model or destroys an operand still borrowed by another executable.
const std = @import("std");
const platform = @import("antfly_platform");
const ml = @import("ml");
const pjrt = @import("pjrt").pjrt;

pub const Store = struct {
    allocator: std.mem.Allocator,
    client: *pjrt.Client,
    mutex: std.atomic.Mutex = .unlocked,
    /// Cold uploads serialize separately; numerical frames never take this lock.
    upload_mutex: std.atomic.Mutex = .unlocked,
    entries: std.StringHashMapUnmanaged(Operand) = .empty,
    uploads: usize = 0,

    const Operand = struct {
        dtype: ml.graph.DType,
        shape: ml.graph.Shape,
        buffer: pjrt.Buffer,
    };
    pub fn init(allocator: std.mem.Allocator, client: *pjrt.Client) Store {
        return .{ .allocator = allocator, .client = client };
    }
    /// Every borrowing executable/frame must have retired before model close.
    pub fn deinit(self: *Store) void {
        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.buffer.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit(self.allocator);
    }
    fn validate(operand: Operand, dtype: ml.graph.DType, dims: []const i64) !void {
        if (operand.dtype != dtype or !std.mem.eql(i64, operand.shape.dims[0..operand.shape.rank()], dims)) return error.ImmutableParameterContractChanged;
    }
    pub fn get(self: *Store, name: []const u8, dtype: ml.graph.DType, dims: []const i64) !?pjrt.Buffer {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const operand = self.entries.get(name) orelse return null;
        try validate(operand, dtype, dims);
        return operand.buffer;
    }
    /// Upload happens outside the metadata lock. On success consumes candidate,
    /// including duplicate publication. Failure leaves it with the caller.
    pub fn publish(self: *Store, name: []const u8, dtype: ml.graph.DType, dims: []const i64, candidate: pjrt.Buffer) !pjrt.Buffer {
        if (dims.len > 8) return error.UnsupportedShape;
        platform.sync.lockYielding(&self.mutex);
        if (self.entries.get(name)) |existing| {
            validate(existing, dtype, dims) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.mutex.unlock();
            var duplicate = candidate;
            duplicate.deinit();
            return existing.buffer;
        }
        const owned_name = self.allocator.dupe(u8, name) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.entries.put(self.allocator, owned_name, .{ .dtype = dtype, .shape = ml.graph.Shape.init(dtype, dims), .buffer = candidate }) catch |err| {
            self.allocator.free(owned_name);
            self.mutex.unlock();
            return err;
        };
        self.uploads += 1;
        self.mutex.unlock();
        return candidate;
    }
};
