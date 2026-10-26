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

//! Canonical lookup of reader registries across hidden static runtime units.
//! Every later operation follows its actual origin bucket/node; only opening by
//! durable owner identity needs this host-supplied lookup. No wire grant exists.
const std = @import("std");
const publication = @import("index_publication.zig");
const runtime = @import("antfly_runtime_abi");
const A = std.mem.Allocator;
pub const Port = runtime.source_owner_abi.Port;
const Registry = publication.SharedReaders.OwnerRegistry;
const Layouts = @TypeOf(publication.SharedReaders.readerLayouts());
const VTable = struct {
    open_owner: *const fn (?*anyopaque, A, publication.Digest, Layouts) anyerror!Registry,
};
const ABI = runtime.callback_abi.Boundary(VTable);
const descriptor: runtime.source_owner_abi.Descriptor = descriptorValue();
fn descriptorValue() runtime.source_owner_abi.Descriptor {
    @setEvalBranchQuota(100_000);
    const Callback = @FieldType(VTable, "open_owner");
    const Function = @typeInfo(Callback).pointer.child;
    const layouts = publication.SharedReaders.readerLayouts();
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.asBytes(&layouts), &digest, .{});
    return .{ .open_owner = .of("open_owner", Callback, std.meta.ArgsTuple(Function), Registry), .reader_layouts = digest };
}
fn compatible(port: Port) bool {
    return port.valid() and port.descriptor.open_owner.matches(descriptor.open_owner) and
        std.mem.eql(u8, &port.descriptor.reader_layouts, &descriptor.reader_layouts);
}
pub const Broker = struct {
    port: Port,
    pub fn local() Broker {
        return .{ .port = .{ .descriptor = &descriptor, .callback = @ptrCast(&openCanonical), .dispatch = ABI.local_dispatch } };
    }
    pub fn receive(port: Port) !Broker {
        if (!compatible(port)) return error.UnsupportedVersion;
        // Descriptor storage may have been borrowed only for this receive.
        // Preserve callbacks/context but retain our checked static descriptor.
        var copied = port;
        copied.descriptor = &descriptor;
        return .{ .port = copied };
    }
    pub fn open(self: Broker, a: A, owner: publication.Digest) !Registry {
        if (!compatible(self.port)) return error.UnsupportedVersion;
        const callback: @FieldType(VTable, "open_owner") = @ptrCast(@alignCast(self.port.callback));
        return ABI.call("open_owner", self.port.dispatch, callback, .{ self.port.context, a, owner, publication.SharedReaders.readerLayouts() });
    }
};
fn openCanonical(_: ?*anyopaque, a: A, owner: publication.Digest, layouts: Layouts) !Registry {
    if (!std.meta.eql(layouts, publication.SharedReaders.readerLayouts())) return error.InvalidArgument;
    return Registry.open(a, owner);
}

test "index source recovery canonical broker ports rendezvous and reject incompatible layouts" {
    const a = std.testing.allocator;
    const owner: publication.Digest = @splat(93);
    const binding: publication.Binding = .{ .owner = owner, .publication = @splat(94) };
    const Foreign = struct {
        fn dispatch(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) runtime.error_abi.Status {
            return ABI.local_dispatch(contract, callback, args, output);
        }
    };
    const canonical = Broker.local();
    var port = canonical.port;
    port.dispatch = Foreign.dispatch;
    const received = try Broker.receive(port);
    var first = try canonical.open(a, owner);
    defer first.deinit();
    var second = try received.open(a, owner);
    defer second.deinit();
    var root = try first.openPublication(a, binding);
    defer root.deinit();
    var held = try root.register();
    defer held.deinit();
    var reopened = try second.openPublication(a, binding);
    defer reopened.deinit();
    reopened.retire();
    try std.testing.expect(reopened.drained() == null);
    held.deinit();
    try std.testing.expect(reopened.drained() != null);
    var wrong = publication.SharedReaders.readerLayouts();
    wrong[0].size += 1;
    try std.testing.expectError(error.InvalidArgument, ABI.call("open_owner", port.dispatch, &openCanonical, .{ null, a, owner, wrong }));
    var transient_descriptor = descriptor;
    var transient_port = port;
    transient_port.descriptor = &transient_descriptor;
    const copied = try Broker.receive(transient_port);
    transient_descriptor.reader_layouts[0] ^= 1;
    var copied_registry = try copied.open(a, owner);
    defer copied_registry.deinit();
    var copied_root = try copied_registry.openPublication(a, binding);
    defer copied_root.deinit();
    try std.testing.expect(copied_root.drained() != null);
    var wrong_descriptor = descriptor;
    wrong_descriptor.reader_layouts[0] ^= 1;
    var wrong_port = port;
    wrong_port.descriptor = &wrong_descriptor;
    try std.testing.expectError(error.UnsupportedVersion, Broker.receive(wrong_port));
    wrong_descriptor = descriptor;
    wrong_descriptor.open_owner.arguments.size += 1;
    try std.testing.expectError(error.UnsupportedVersion, Broker.receive(wrong_port));
    port.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, Broker.receive(port));
}
