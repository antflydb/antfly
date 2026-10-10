// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const api = @import("extraction_api.zig");
const host = @import("host_abi.zig");
const Len = host.HostLen;
export fn extraction_abi_version() u32 {
    return 1;
}
export fn extraction_create(ptr: [*]const u8, len: Len) u32 {
    return api.create(host.sliceConst(u8, ptr, len)) catch |err| api.fail(err);
}
export fn extraction_tokenizer(handle: u32, ptr: [*]const u8, len: Len) u32 {
    api.tokenizer(handle, host.sliceConst(u8, ptr, len)) catch |err| return api.fail(err);
    return 1;
}
export fn extraction_weight(handle: u32, meta: [*]const u8, meta_len: Len, ptr: [*]const u8, len: Len) u32 {
    api.weight(handle, host.sliceConst(u8, meta, meta_len), host.sliceConst(u8, ptr, len)) catch |err| return api.fail(err);
    return 1;
}
export fn extraction_finalize(handle: u32, digest: [*]const u8, size: u64) u32 {
    api.finalize(handle, digest[0..64], size) catch |err| return api.fail(err);
    return 1;
}
export fn extraction_run(handle: u32, ptr: [*]const u8, len: Len, validate_only: u32) u32 {
    api.run(handle, host.sliceConst(u8, ptr, len), validate_only != 0) catch |err| return api.fail(err);
    return 1;
}
export fn extraction_result_ptr() [*]const u8 {
    return api.result.ptr;
}
export fn extraction_result_len() Len {
    return @intCast(api.result.len);
}
export fn extraction_result_free() void {
    api.clearResult();
}
export fn extraction_live_bytes(handle: u32) Len {
    const model = api.get(handle) catch return 0;
    return @intCast(model.budget.live);
}
export fn extraction_error_ptr() [*]const u8 {
    return api.last_error.ptr;
}
export fn extraction_error_len() Len {
    return @intCast(api.last_error.len);
}
export fn extraction_unload() void {
    api.unload();
}
export fn extraction_hash_begin() void {
    api.hashBegin();
}
export fn extraction_hash_update(ptr: [*]const u8, len: Len) void {
    api.hashUpdate(host.sliceConst(u8, ptr, len));
}
export fn extraction_hash_end() [*]const u8 {
    return api.hashEnd().ptr;
}
