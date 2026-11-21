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
        shape: ml.graph.Shape,
        // A checkpoint operand has one immutable logical shape, with an
        // uploaded representation for each dtype actually used by a stage.
        buffers: std.EnumArray(ml.graph.DType, ?pjrt.Buffer) = .initFill(null),
    };
    pub fn init(allocator: std.mem.Allocator, client: *pjrt.Client) Store {
        return .{ .allocator = allocator, .client = client };
    }
    /// Every borrowing executable/frame must have retired before model close.
    pub fn deinit(self: *Store) void {
        var iterator = self.entries.iterator();
        while (iterator.next()) |entry| {
            for (&entry.value_ptr.buffers.values) |*variant| if (variant.*) |*buffer| buffer.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit(self.allocator);
    }
    fn validate(operand: Operand, dims: []const i64) !void {
        if (!std.mem.eql(i64, operand.shape.dims[0..operand.shape.rank()], dims)) return error.ImmutableParameterContractChanged;
    }
    pub fn get(self: *Store, name: []const u8, dtype: ml.graph.DType, dims: []const i64) !?pjrt.Buffer {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const operand = self.entries.get(name) orelse return null;
        try validate(operand, dims);
        return operand.buffers.get(dtype);
    }
    /// Upload happens outside the metadata lock. On success consumes candidate,
    /// including duplicate publication. Failure leaves it with the caller.
    pub fn publish(self: *Store, name: []const u8, dtype: ml.graph.DType, dims: []const i64, candidate: pjrt.Buffer) !pjrt.Buffer {
        if (dims.len > 8) return error.UnsupportedShape;
        platform.sync.lockYielding(&self.mutex);
        if (self.entries.getPtr(name)) |existing| {
            validate(existing.*, dims) catch |err| {
                self.mutex.unlock();
                return err;
            };
            if (existing.buffers.get(dtype)) |buffer| {
                self.mutex.unlock();
                var duplicate = candidate;
                duplicate.deinit();
                return buffer;
            }
            existing.buffers.set(dtype, candidate);
            self.uploads += 1;
            self.mutex.unlock();
            return candidate;
        }
        const owned_name = self.allocator.dupe(u8, name) catch |err| {
            self.mutex.unlock();
            return err;
        };
        var operand = Operand{ .shape = ml.graph.Shape.init(dtype, dims) };
        operand.buffers.set(dtype, candidate);
        self.entries.put(self.allocator, owned_name, operand) catch |err| {
            self.allocator.free(owned_name);
            self.mutex.unlock();
            return err;
        };
        self.uploads += 1;
        self.mutex.unlock();
        return candidate;
    }
};
