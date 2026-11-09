// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Offline reader load-phase diagnostic. No decoder or serving state is used.
const std = @import("std");
const internal = @import("inference_internal");
const CudaCompute = internal.native_compute.cuda.CudaCompute;
const model = internal.models.memorilla;
const A = std.mem.Allocator;

fn millis(from: std.Io.Timestamp, to: std.Io.Timestamp) f64 {
    return @as(f64, @floatFromInt(from.durationTo(to).nanoseconds)) / std.time.ns_per_ms;
}

pub fn main(init: std.process.Init) !void {
    const a = std.heap.c_allocator;
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next();
    const directory = args.next() orelse return error.ExpectedBundleDirectory;
    if (args.next() != null) return error.InvalidArguments;
    for (0..3) |iteration| {
        const start = std.Io.Timestamp.now(init.io, .awake);
        var bundle = try model.Bundle.open(a, directory, false);
        if (bundle.reader.mmap_region) |*mapped| mapped.discard_on_deinit = false;
        const opened = std.Io.Timestamp.now(init.io, .awake);
        var store = try bundle.loadFloat32Store();
        const converted = std.Io.Timestamp.now(init.io, .awake);
        var compute = try CudaCompute.initWithDeviceMemoryLimit(a, 2 * 1024 * 1024 * 1024);
        compute.strict_f32_weights = true;
        compute.capture_gates = .{};
        const initialized = std.Io.Timestamp.now(init.io, .awake);
        var iter = store.resident_weights.iterator();
        while (iter.next()) |entry| try compute.insertWeightFromLoaded(try a.dupe(u8, entry.key_ptr.*), entry.value_ptr);
        const uploaded = std.Io.Timestamp.now(init.io, .awake);
        if (compute.snapshotStats().resident_weight_bytes != internal.models.memorilla_runtime.parameter_bytes) return error.InvalidResidentWeightInventory;
        compute.deinit();
        const retired = std.Io.Timestamp.now(init.io, .awake);
        store.deinitOwned();
        bundle.deinit();
        const done = std.Io.Timestamp.now(init.io, .awake);
        const bytes = try std.json.Stringify.valueAlloc(a, .{
            .iteration = iteration,
            .bundle_open_ms = millis(start, opened),
            .convert_ms = millis(opened, converted),
            .cuda_init_ms = millis(converted, initialized),
            .upload_ms = millis(initialized, uploaded),
            .cuda_retire_ms = millis(uploaded, retired),
            .host_retire_ms = millis(retired, done),
        }, .{});
        defer a.free(bytes);
        try std.Io.File.stdout().writeStreamingAll(init.io, bytes);
        try std.Io.File.stdout().writeStreamingAll(init.io, "\n");
    }
}
