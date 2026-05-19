// Copyright 2026 Antfly, Inc.
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

//! End-to-end smoke test for the protoc-zig output.
//!
//! This test is compiled by `build.zig` against the *generated* module produced
//! by running `protoc-zig` over `src/testdata/quantize.desc`. It confirms that
//! the emitted Zig source:
//!   1. Compiles against the real `protobuf` runtime module,
//!   2. Honours the `_pb_field_map` comptime convention, and
//!   3. Round-trips through `encode` / `decode` / `deinit` without allocator
//!      leaks.
//!
//! The generated module is wired in via `@import("generated")`. `build.zig`
//! builds the module by running the `protoc-zig` artifact and pointing
//! `root_source_file` at the emitted `root.zig`.

const std = @import("std");
const testing = std.testing;

const generated = @import("generated");

test "generated RaBitQCodeSet round-trips" {
    const alloc = testing.allocator;

    const quantize = generated.antfly_lib_vector_quantize;
    const RaBitQCodeSet = quantize.RaBitQCodeSet;

    var data_buf = [_]u64{ 0x0123456789ABCDEF, 0xFEDCBA9876543210, 42 };
    var original: RaBitQCodeSet = .{
        .count = 3,
        .width = 64,
        .data = data_buf[0..],
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);
    try testing.expect(bytes.len > 0);

    var decoded = try RaBitQCodeSet.decode(alloc, bytes);
    defer decoded.deinit(alloc);

    try testing.expectEqual(@as(i64, 3), decoded.count);
    try testing.expectEqual(@as(i64, 64), decoded.width);
    try testing.expectEqual(@as(usize, 3), decoded.data.len);
    try testing.expectEqualSlices(u64, data_buf[0..], decoded.data);
}

test "generated Set round-trips" {
    const alloc = testing.allocator;

    const vector = generated.antfly_lib_vector;
    const Set = vector.Set;

    var float_buf = [_]f32{ 1.0, -2.5, 3.25, 0, 7.125 };
    var original: Set = .{
        .dims = 5,
        .count = 1,
        .data = float_buf[0..],
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);

    var decoded = try Set.decode(alloc, bytes);
    defer decoded.deinit(alloc);

    try testing.expectEqual(@as(i64, 5), decoded.dims);
    try testing.expectEqual(@as(i64, 1), decoded.count);
    try testing.expectEqualSlices(f32, float_buf[0..], decoded.data);
}

test "generated cross-package reference (NonQuantizedVectorSet)" {
    const alloc = testing.allocator;

    const vector = generated.antfly_lib_vector;
    const quantize = generated.antfly_lib_vector_quantize;
    const NonQuantizedVectorSet = quantize.NonQuantizedVectorSet;

    var float_buf = [_]f32{ 0.5, 1.5, 2.5, 3.5 };
    var original: NonQuantizedVectorSet = .{
        .vectors = vector.Set{
            .dims = 2,
            .count = 2,
            .data = float_buf[0..],
        },
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);

    var decoded = try NonQuantizedVectorSet.decode(alloc, bytes);
    defer decoded.deinit(alloc);

    try testing.expectEqual(@as(i64, 2), decoded.vectors.dims);
    try testing.expectEqual(@as(i64, 2), decoded.vectors.count);
    try testing.expectEqualSlices(f32, float_buf[0..], decoded.vectors.data);
}

test "generated enum default matches @enumFromInt(0)" {
    const vector = generated.antfly_lib_vector;

    // RotAlgorithm.None == 0
    const none: vector.RotAlgorithm = @enumFromInt(0);
    try testing.expectEqual(vector.RotAlgorithm.None, none);

    // DistanceMetric.L2Squared == 0
    const l2: vector.DistanceMetric = @enumFromInt(0);
    try testing.expectEqual(vector.DistanceMetric.L2Squared, l2);
}

test "generated RaBitQuantizedVectorSet full round-trip with enums and nested submessage" {
    const alloc = testing.allocator;

    const vector = generated.antfly_lib_vector;
    const quantize = generated.antfly_lib_vector_quantize;
    const RaBitQuantizedVectorSet = quantize.RaBitQuantizedVectorSet;
    const RaBitQCodeSet = quantize.RaBitQCodeSet;

    var centroid_buf = [_]f32{ 1.0, 2.0, 3.0 };
    var code_counts_buf = [_]u32{ 10, 20, 30 };
    var dists_buf = [_]f32{ 0.1, 0.2, 0.3 };
    var qdot_buf = [_]f32{ 0.4, 0.5, 0.6 };
    var cdot_buf = [_]f32{ 0.7, 0.8, 0.9 };
    var code_data_buf = [_]u64{ 0xCAFEBABE, 0xDEADBEEF };

    var original: RaBitQuantizedVectorSet = .{
        .metric = vector.DistanceMetric.Cosine,
        .centroid = centroid_buf[0..],
        .codes = RaBitQCodeSet{
            .count = 2,
            .width = 32,
            .data = code_data_buf[0..],
        },
        .code_counts = code_counts_buf[0..],
        .centroid_distances = dists_buf[0..],
        .quantized_dot_products = qdot_buf[0..],
        .centroid_dot_products = cdot_buf[0..],
        .centroid_norm = 1.5,
    };

    const bytes = try original.encode(alloc);
    defer alloc.free(bytes);

    var decoded = try RaBitQuantizedVectorSet.decode(alloc, bytes);
    defer decoded.deinit(alloc);

    try testing.expectEqual(vector.DistanceMetric.Cosine, decoded.metric);
    try testing.expectEqualSlices(f32, centroid_buf[0..], decoded.centroid);
    try testing.expectEqual(@as(i64, 2), decoded.codes.count);
    try testing.expectEqual(@as(i64, 32), decoded.codes.width);
    try testing.expectEqualSlices(u64, code_data_buf[0..], decoded.codes.data);
    try testing.expectEqualSlices(u32, code_counts_buf[0..], decoded.code_counts);
    try testing.expectEqualSlices(f32, dists_buf[0..], decoded.centroid_distances);
    try testing.expectEqualSlices(f32, qdot_buf[0..], decoded.quantized_dot_products);
    try testing.expectEqualSlices(f32, cdot_buf[0..], decoded.centroid_dot_products);
    try testing.expectEqual(@as(f32, 1.5), decoded.centroid_norm);
}
