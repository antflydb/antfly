// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Shared dynamically loaded cuDNN backend ABI. Descriptor owners remain with
//! the operation module; this interface neither creates a handle nor loads an SDK.
const std = @import("std");
const platform = @import("antfly_platform");
pub const Desc = ?*anyopaque;
pub const Status = c_int;
// cudnn_graph.h, captured SDK SHA75a70552a07660d25ad900dd45f180ec557965fd41433be8723bbd4c64337308.
pub const DescriptorType = enum(c_int) {
    convolution = 1,
    engine = 2,
    enginecfg = 3,
    engineheur = 4,
    execution_plan = 5,
    convolution_fwd = 10,
    operation_graph = 15,
    variant_pack = 16,
    tensor = 17,
    sdpa_fwd = 41,
    diagonal_band = 44,
};
pub const AttributeType = enum(c_int) {
    handle = 0,
    data_type = 1,
    boolean = 2,
    int64 = 3,
    float = 4,
    void_ptr = 6,
    convolution_mode = 7,
    heur_mode = 8,
    numerical_note = 11,
    pointwise_mode = 14,
    backend_descriptor = 15,
};

pub const Api = struct {
    create_desc: *const fn (DescriptorType, *Desc) callconv(.c) Status,
    destroy_desc: *const fn (Desc) callconv(.c) Status,
    set: *const fn (Desc, c_int, AttributeType, i64, ?*const anyopaque) callconv(.c) Status,
    get: *const fn (Desc, c_int, AttributeType, i64, *i64, ?*anyopaque) callconv(.c) Status,
    finalize: *const fn (Desc) callconv(.c) Status,
    execute: *const fn (Desc, Desc, Desc) callconv(.c) Status,

    pub fn lookup(lib: *std.DynLib) ?Api {
        var result: Api = undefined;
        inline for (.{ "cudnnBackendCreateDescriptor", "cudnnBackendDestroyDescriptor", "cudnnBackendSetAttribute", "cudnnBackendGetAttribute", "cudnnBackendFinalize", "cudnnBackendExecute" }, @typeInfo(Api).@"struct".field_names) |symbol, field| {
            @field(result, field) = lib.lookup(@FieldType(Api, field), symbol) orelse return null;
        }
        return result;
    }
    pub fn check(status: Status) !void {
        if (status != 0) return error.CudnnFailure;
    }
    pub fn destroyChecked(api: *const Api, owner: *Desc) !void {
        if (owner.* == null) return;
        if (api.destroy_desc(owner.*) != 0) return error.CudnnDescriptorDestroyFailed;
        owner.* = null;
    }
    pub fn destroy(api: *const Api, owner: *Desc) void {
        api.destroyChecked(owner) catch platform.inference_process_supervisor.restartWorker();
    }
};

test "backend descriptor destruction failure retains the exact owner for retry" {
    const Fake = struct {
        var status: Status = 0;
        fn destroy(_: Desc) callconv(.c) Status {
            return status;
        }
    };
    var api: Api = undefined;
    api.destroy_desc = Fake.destroy;
    var owner: Desc = @ptrFromInt(0x1000);
    Fake.status = 1;
    try std.testing.expectError(error.CudnnDescriptorDestroyFailed, api.destroyChecked(&owner));
    try std.testing.expectEqual(@as(usize, 0x1000), @intFromPtr(owner.?));
    Fake.status = 0;
    try api.destroyChecked(&owner);
    try std.testing.expect(owner == null);
}
