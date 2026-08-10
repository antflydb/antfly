// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");

const platform_time = @import("antfly_platform").time;
const runtime_schema = @import("../storage/schema.zig");

const ns_per_day: u64 = 86_400 * std.time.ns_per_s;

pub fn relationalDefaultValueJsonAlloc(
    alloc: std.mem.Allocator,
    default_value: runtime_schema.RelationalDefaultValue,
) ![]u8 {
    return switch (default_value.kind) {
        .literal => try alloc.dupe(u8, default_value.value_json),
        .now_ns => try std.fmt.allocPrint(alloc, "{d}", .{platform_time.realtimeNs()}),
        .current_date_ns => try std.fmt.allocPrint(alloc, "{d}", .{currentUtcDateStartNs()}),
        .uuid_v4 => blk: {
            const uuid = try uuidV4String();
            break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(uuid[0..], .{})});
        },
        .sequence_next, .scalar_subquery => error.UnsupportedSqlShape,
    };
}

fn currentUtcDateStartNs() u64 {
    const now_ns = platform_time.realtimeNs();
    return now_ns - (now_ns % ns_per_day);
}

pub fn uuidV4String() ![36]u8 {
    var bytes: [16]u8 = undefined;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try io_impl.io().randomSecure(&bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    var out: [36]u8 = undefined;
    const hex = "0123456789abcdef";
    var src: usize = 0;
    var dst: usize = 0;
    while (src < bytes.len) : (src += 1) {
        if (dst == 8 or dst == 13 or dst == 18 or dst == 23) {
            out[dst] = '-';
            dst += 1;
        }
        out[dst] = hex[bytes[src] >> 4];
        out[dst + 1] = hex[bytes[src] & 0x0f];
        dst += 2;
    }
    return out;
}
