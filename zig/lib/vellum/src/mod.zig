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

//! Vellum-compatible FST (Finite State Transducer) implementation.
//!
//! Wire-compatible with github.com/blevesearch/vellum v1 format.
//! Supports building FSTs from sorted key/value pairs, exact lookup,
//! range iteration, and automaton-based search.
//!
//! File layout:
//!   [Header: 16 bytes][FST states (variable)][Footer: 16 bytes]
//!
//! Header: [version: u64 LE][type: u64 LE]
//! Footer: [count: u64 LE][root_addr: u64 LE]

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

// ============================================================================
// Constants
// ============================================================================

const header_size: usize = 16;
const footer_size: usize = 16;
const version_v1: u64 = 1;

const one_transition: u8 = 1 << 7;
const transition_next: u8 = 1 << 6;
const state_final: u8 = 1 << 6;
const max_common: u8 = (1 << 6) - 1;
const max_num_trans: u8 = (1 << 6) - 1;

const none_addr: usize = 1;
const empty_addr: usize = 0;

// ============================================================================
// Common input encoding (Vellum-compatible byte frequency table)
// ============================================================================

/// Maps byte value → common code. 0 means uncommon.
const common_inputs: [256]u8 = .{
    84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, // 0x00-0x0f
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, // 0x10-0x1f
    116, 80, 117, 118, 79, 39, 30, 81, 75, 74, 82, 57, 66, 16, 12, 2, // ' '-'/'
    19, 20, 21, 27, 32, 29, 35, 36, 37, 34, 24, 73, 119, 23, 120, 40, // '0'-'?'
    83, 44, 48, 42, 43, 49, 46, 62, 61, 47, 69, 68, 58, 56, 55, 59, // '@'-'O'
    51, 72, 54, 45, 52, 64, 65, 63, 71, 67, 70, 77, 121, 78, 122, 31, // 'P'-'_'
    123, 4, 25, 9, 17, 1, 26, 22, 13, 7, 50, 38, 14, 15, 10, 3, // '`'-'o'
    8,   60,  6,   5,   0,   18,  33,  11,  41,  28,  53,  124, 125, 126, 76,  127, // 'p'-0x7f
    128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143,
    144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159,
    160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175,
    176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191,
    192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207,
    208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
    224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
    240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 252, 253, 254, 255,
};

/// Maps common code (1-based) → original byte value.
const common_inputs_inv: [127]u8 = .{
    't',  'e',  '/',  'o',  'a',  's',  'r',  'i',  'p',  'c',  'n',  'w',  '.',  'h',  'l',  'm',
    '-',  'd',  'u',  '0',  '1',  '2',  'g',  '=',  ':',  'b',  'f',  '3',  'y',  '5',  '&',  '_',
    '4',  'v',  '9',  '6',  '7',  '8',  'k',  '%',  '?',  'x',  'C',  'D',  'A',  'S',  'F',  'I',
    'B',  'E',  'j',  'P',  'T',  'z',  'R',  'N',  'M',  '+',  'L',  'O',  'q',  'H',  'G',  'W',
    'U',  'V',  ',',  'Y',  'K',  'J',  'Z',  'X',  'Q',  ';',  ')',  '(',  '~',  '[',  ']',  '$',
    '!',  '\'', '*',  '@',  0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, '\t', '\n', 0x0b,
    0x0c, '\r', 0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
    0x1c, 0x1d, 0x1e, 0x1f, ' ',  '"',  '#',  '<',  '>',  '\\', '^',  '`',  '{',  '|',  '}',
};

fn encodeCommon(in: u8) u8 {
    const val: u8 = @truncate((@as(u16, common_inputs[in]) + 1) % 256);
    if (val > max_common) return 0;
    return val;
}

fn decodeCommon(in: u8) u8 {
    return common_inputs_inv[in - 1];
}

// ============================================================================
// Pack utilities
// ============================================================================

fn packedSize(n: u64) u8 {
    if (n < @as(u64, 1) << 8) return 1;
    if (n < @as(u64, 1) << 16) return 2;
    if (n < @as(u64, 1) << 24) return 3;
    if (n < @as(u64, 1) << 32) return 4;
    if (n < @as(u64, 1) << 40) return 5;
    if (n < @as(u64, 1) << 48) return 6;
    if (n < @as(u64, 1) << 56) return 7;
    return 8;
}

fn readPackedUint(data: []const u8) u64 {
    var rv: u64 = 0;
    for (data, 0..) |b, i| {
        rv |= @as(u64, b) << @intCast(i * 8);
    }
    return rv;
}

fn encodePackSize(trans_size: u8, out_size: u8) u8 {
    return (trans_size << 4) | out_size;
}

fn decodePackSizeTrans(pack: u8) u8 {
    return pack >> 4;
}

fn decodePackSizeOut(pack: u8) u8 {
    return pack & 0x0f;
}

fn deltaAddr(base: u64, target: u64) u64 {
    if (target == 0) return 0;
    return base - target;
}

// ============================================================================
// Output arithmetic (for transducer output factoring)
// ============================================================================

fn outputPrefix(l: u64, r: u64) u64 {
    return @min(l, r);
}

fn outputSub(l: u64, r: u64) u64 {
    return l - r;
}

fn outputCat(l: u64, r: u64) u64 {
    return l + r;
}

// ============================================================================
// Builder
// ============================================================================

const Transition = struct {
    out: u64 = 0,
    addr: usize = none_addr,
    in: u8 = 0,
};

const BuilderNode = struct {
    final_output: u64 = 0,
    trans: std.ArrayListUnmanaged(Transition) = .empty,
    final: bool = false,

    fn deinit(self: *BuilderNode, alloc: Allocator) void {
        self.trans.deinit(alloc);
    }

    fn reset(self: *BuilderNode) void {
        self.final = false;
        self.final_output = 0;
        self.trans.clearRetainingCapacity();
    }

    fn equiv(self: *const BuilderNode, other: *const BuilderNode) bool {
        if (self.final != other.final) return false;
        if (self.final_output != other.final_output) return false;
        if (self.trans.items.len != other.trans.items.len) return false;
        for (self.trans.items, other.trans.items) |a, b| {
            if (a.in != b.in or a.addr != b.addr or a.out != b.out) return false;
        }
        return true;
    }
};

const BuilderNodeUnfinished = struct {
    node: BuilderNode = .{},
    last_out: u64 = 0,
    last_in: u8 = 0,
    has_last_t: bool = false,

    fn lastCompiled(self: *BuilderNodeUnfinished, alloc: Allocator, addr: usize) !void {
        if (self.has_last_t) {
            self.has_last_t = false;
            try self.node.trans.append(alloc, .{
                .in = self.last_in,
                .out = self.last_out,
                .addr = addr,
            });
            self.last_out = 0;
        }
    }

    fn addOutputPrefix(self: *BuilderNodeUnfinished, prefix: u64) void {
        if (self.node.final) {
            self.node.final_output = outputCat(prefix, self.node.final_output);
        }
        for (self.node.trans.items) |*t| {
            t.out = outputCat(prefix, t.out);
        }
        if (self.has_last_t) {
            self.last_out = outputCat(prefix, self.last_out);
        }
    }
};

/// Registry for deduplicating equivalent FST states during build.
///
/// The cell table comes from `std.heap.PageAllocator.map`, the raw map/unmap
/// primitive that backs the page allocator (mmap on POSIX, NtAllocateVirtualMemory
/// on Windows). Bypassing `Allocator.alloc` matters because that wrapper does a
/// `@memset(undefined)` after every allocation, which would mask the gen==0
/// invariant below. Going through `map` directly preserves the kernel's
/// zero-on-first-touch guarantee, so cells (and the pages backing them) the
/// build never visits cost nothing.
///
/// **Soundness invariant:** `cell.node` is only ever read for cells whose
/// `cell.gen != 0`. Zero-initialized memory from the OS is read *only* as
/// `u32 cell.gen`, which is well-defined to be 0. The optional / pointer
/// fields inside the cell are only inspected after Zig code has actually
/// written them, so we never reinterpret raw OS memory as `?BuilderNode`.
///
/// `cell.gen` doubles as an O(1) reset mechanism:
///   - `cell.gen == 0`             → never written by any build (pristine).
///   - `cell.gen == registry.gen`  → occupied for the current build.
///   - else                        → leftover from a prior build; `cell.node`
///                                   holds an allocated BuilderNode that must
///                                   be freed before the cell is reused.
const Registry = struct {
    /// Allocator for the BuilderNode contents stored in cells. The table
    /// itself comes from `mapZeroTable`, which goes through PageAllocator's
    /// raw map/unmap primitives so cells start as kernel-zeroed pages.
    alloc: Allocator,
    table: []RegistryCell,
    table_size: usize,
    mru_size: usize,
    /// Build epoch. Always `>= 1` so that `gen == 0` (the bit pattern of fresh
    /// OS pages) unambiguously means "never written".
    gen: u32,

    const RegistryCell = struct {
        addr: usize = 0,
        node: ?BuilderNode = null,
        /// Generation stamp. `0` means the cell has never been written by any
        /// build (and reading `node` is **not** allowed). Otherwise the cell
        /// holds a real `BuilderNode` value, occupied for the current build
        /// iff `gen == Registry.gen`.
        gen: u32 = 0,
    };

    fn init(alloc: Allocator, table_size: usize, mru_size: usize) !Registry {
        // Validate options up front: callers can pass arbitrary values via
        // Builder.Options, and table_size * mru_size or n * sizeof can wrap
        // in ReleaseFast and produce a slice longer than the actual mapping.
        const n = std.math.mul(usize, table_size, mru_size) catch return error.RegistryTooLarge;
        const table = try mapZeroTable(alloc, n);
        return .{
            .alloc = alloc,
            .table = table,
            .table_size = table_size,
            .mru_size = mru_size,
            .gen = 1,
        };
    }

    fn deinit(self: *Registry) void {
        for (self.table) |*cell| {
            // Skip cells that have never been written: their `node` field is
            // pristine kernel-zeroed memory we mustn't interpret as ?BuilderNode.
            if (cell.gen == 0) continue;
            if (cell.node) |*n| n.deinit(self.alloc);
        }
        unmapZeroTable(self.alloc, self.table);
    }

    fn mapZeroTable(alloc: Allocator, n: usize) ![]RegistryCell {
        if (n == 0) return &[_]RegistryCell{};
        const byte_count = std.math.mul(usize, n, @sizeOf(RegistryCell)) catch return error.RegistryTooLarge;
        if (comptime builtin.os.tag == .freestanding) {
            const table = try alloc.alloc(RegistryCell, n);
            @memset(std.mem.sliceAsBytes(table), 0);
            return table;
        }
        // Use PageAllocator's raw map/unmap pair so we get kernel-zeroed pages
        // on every supported platform (mmap on POSIX, NtAllocateVirtualMemory
        // on Windows) without going through Allocator.alloc, which inserts a
        // `@memset(undefined)` and would mask our gen==0 invariant.
        const alignment = std.mem.Alignment.fromByteUnits(@alignOf(RegistryCell));
        const raw = std.heap.PageAllocator.map(byte_count, alignment) orelse return error.OutOfMemory;
        const ptr: [*]RegistryCell = @ptrCast(@alignCast(raw));
        return ptr[0..n];
    }

    fn unmapZeroTable(alloc: Allocator, table: []RegistryCell) void {
        if (table.len == 0) return;
        if (comptime builtin.os.tag == .freestanding) {
            alloc.free(table);
            return;
        }
        // No overflow risk: we only get here if the matching mapZeroTable call
        // already validated the same multiplication.
        const byte_count = table.len * @sizeOf(RegistryCell);
        const bytes_ptr: [*]align(std.heap.page_size_min) u8 = @ptrCast(@alignCast(table.ptr));
        std.heap.PageAllocator.unmap(bytes_ptr[0..byte_count]);
    }

    /// Invalidate every cell in O(1) by bumping the generation. On the rare
    /// rollover from `maxInt(u32)`, sweep the table and free any leftover
    /// nodes so the next build can't get a phantom hit on a stale cell.
    fn reset(self: *Registry) void {
        if (self.gen == std.math.maxInt(u32)) {
            for (self.table) |*cell| {
                if (cell.gen == 0) continue;
                if (cell.node) |*n| n.deinit(self.alloc);
                cell.* = .{}; // initialize via Zig assignment, not raw bytes
            }
            self.gen = 1;
        } else {
            self.gen += 1;
        }
    }

    const fnv_prime: u64 = 1099511628211;
    const fnv_offset: u64 = 14695981039346656037;

    fn hash(self: *const Registry, node: *const BuilderNode) usize {
        var final_val: u64 = 0;
        if (node.final) final_val = 1;

        var h: u64 = fnv_offset;
        h = (h ^ final_val) *% fnv_prime;
        h = (h ^ node.final_output) *% fnv_prime;
        for (node.trans.items) |t| {
            h = (h ^ @as(u64, t.in)) *% fnv_prime;
            h = (h ^ t.out) *% fnv_prime;
            h = (h ^ @as(u64, t.addr)) *% fnv_prime;
        }
        return @intCast(h % @as(u64, @intCast(self.table_size)));
    }

    /// Check if an equivalent node exists. Returns (found, addr, cell_to_fill).
    fn entry(self: *Registry, node: *const BuilderNode) struct { found: bool, addr: usize, cell_idx: ?usize } {
        if (self.table.len == 0) return .{ .found = false, .addr = 0, .cell_idx = null };

        const bucket = self.hash(node);
        const start = self.mru_size * bucket;
        const end = start + self.mru_size;

        // Sweep the bucket for a match. Cells with gen==0 are pristine OS
        // memory; we mustn't read .node from them. Cells with non-zero but
        // stale gen hold a leftover BuilderNode we need to free before
        // treating the slot as empty.
        for (start..end) |i| {
            const cell = &self.table[i];
            if (cell.gen == 0) continue;
            if (cell.gen != self.gen) {
                if (cell.node) |*n| n.deinit(self.alloc);
                cell.* = .{}; // resets gen=0, node=null, addr=0
                continue;
            }
            if (cell.node) |*existing| {
                if (existing.equiv(node)) {
                    const addr = cell.addr;
                    self.promote(start, i);
                    return .{ .found = true, .addr = addr, .cell_idx = null };
                }
            }
        }

        // No match - evict LRU (last in bucket). After the sweep above, the
        // last cell is either gen==self.gen (the live MRU) or gen==0 (pristine
        // / just cleaned). In either case a follow-up insert at cell_idx will
        // overwrite the slot.
        const last = end - 1;
        if (self.table[last].gen == self.gen) {
            if (self.table[last].node) |*n| n.deinit(self.alloc);
        }
        self.table[last] = .{};
        self.promote(start, last);
        return .{ .found = false, .addr = 0, .cell_idx = start };
    }

    fn promote(self: *Registry, start: usize, i: usize) void {
        var j = i;
        while (j > start) : (j -= 1) {
            const tmp = self.table[j - 1];
            self.table[j - 1] = self.table[j];
            self.table[j] = tmp;
        }
    }
};

/// Builds a Vellum-compatible FST. Keys must be inserted in lexicographic order.
pub const Builder = struct {
    alloc: Allocator,
    stack: std.ArrayListUnmanaged(BuilderNodeUnfinished),
    registry: Registry,
    last: std.ArrayListUnmanaged(u8),
    count: usize = 0,
    last_addr: usize = none_addr,
    out: std.ArrayListUnmanaged(u8),
    counter: usize = 0, // bytes written so far (after header)

    pub const Options = struct {
        registry_table_size: usize = 10000,
        registry_mru_size: usize = 2,
    };

    pub fn init(alloc: Allocator, opts: Options) !Builder {
        var b = Builder{
            .alloc = alloc,
            .stack = .empty,
            .registry = try Registry.init(alloc, opts.registry_table_size, opts.registry_mru_size),
            .last = .empty,
            .out = .empty,
        };

        // Write header
        try b.out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, version_v1)));
        try b.out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, 0))); // type

        // Push initial empty root node
        try b.stack.append(alloc, .{ .node = .{ .final = false } });

        return b;
    }

    pub fn deinit(self: *Builder) void {
        for (self.stack.items) |*item| item.node.deinit(self.alloc);
        self.stack.deinit(self.alloc);
        self.registry.deinit();
        self.last.deinit(self.alloc);
        self.out.deinit(self.alloc);
    }

    /// Re-initialize the builder for a new FST without freeing the registry
    /// table or the output buffer. After reset, the builder behaves exactly
    /// as if it were freshly initialized — but the expensive page-backed
    /// registry table is reused, and growable buffers keep their capacity.
    /// Useful for callers that build many segments back-to-back.
    pub fn reset(self: *Builder) !void {
        for (self.stack.items) |*item| item.node.deinit(self.alloc);
        self.stack.clearRetainingCapacity();
        self.last.clearRetainingCapacity();
        self.out.clearRetainingCapacity();
        self.count = 0;
        self.last_addr = none_addr;
        self.counter = 0;
        self.registry.reset();

        try self.out.appendSlice(self.alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, version_v1)));
        try self.out.appendSlice(self.alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, 0)));
        try self.stack.append(self.alloc, .{ .node = .{ .final = false } });
    }

    /// Insert a key/value pair. Keys MUST be inserted in lexicographic order.
    pub fn insert(self: *Builder, key: []const u8, val: u64) !void {
        // Ensure lexicographic order
        if (std.mem.order(u8, key, self.last.items) == .lt) return error.OutOfOrder;

        if (key.len == 0) {
            self.count = 1;
            self.stack.items[0].node.final = true;
            self.stack.items[0].node.final_output = val;
            return;
        }

        var prefix_len: usize = 0;
        var out = val;

        // findCommonPrefixAndSetOutput
        while (prefix_len < key.len and prefix_len < self.stack.items.len) {
            if (!self.stack.items[prefix_len].has_last_t) break;
            if (self.stack.items[prefix_len].last_in != key[prefix_len]) break;

            const common_pre = outputPrefix(self.stack.items[prefix_len].last_out, out);
            const add_prefix = outputSub(self.stack.items[prefix_len].last_out, common_pre);
            out = outputSub(out, common_pre);
            self.stack.items[prefix_len].last_out = common_pre;
            prefix_len += 1;

            if (add_prefix != 0) {
                self.stack.items[prefix_len].addOutputPrefix(add_prefix);
            }
        }

        self.count += 1;
        try self.compileFrom(prefix_len);

        // copyLastKey
        self.last.clearRetainingCapacity();
        try self.last.appendSlice(self.alloc, key);

        // addSuffix
        if (key.len > prefix_len) {
            const last_idx = self.stack.items.len - 1;
            self.stack.items[last_idx].has_last_t = true;
            self.stack.items[last_idx].last_in = key[prefix_len];
            self.stack.items[last_idx].last_out = out;

            for (key[prefix_len + 1 ..]) |b| {
                try self.stack.append(self.alloc, .{
                    .node = .{},
                    .has_last_t = true,
                    .last_in = b,
                    .last_out = 0,
                });
            }
            // Push final empty node
            try self.stack.append(self.alloc, .{ .node = .{ .final = true } });
        }
    }

    /// Finalize the FST. Returns the complete FST bytes. Caller owns result.
    pub fn finish(self: *Builder) ![]u8 {
        try self.compileFrom(0);

        // Pop root
        const root_unfinished = self.stack.items[self.stack.items.len - 1];
        self.stack.items.len -= 1;
        var root_node = root_unfinished.node;
        defer root_node.deinit(self.alloc);

        const root_addr = try self.compile(&root_node);

        // Write footer
        try self.out.appendSlice(self.alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, @intCast(self.count))));
        try self.out.appendSlice(self.alloc, &std.mem.toBytes(std.mem.nativeToLittle(u64, @intCast(root_addr))));

        return try self.alloc.dupe(u8, self.out.items);
    }

    fn compileFrom(self: *Builder, i_state: usize) !void {
        var addr: usize = none_addr;
        while (i_state + 1 < self.stack.items.len) {
            var unfinished = self.stack.items[self.stack.items.len - 1];
            self.stack.items.len -= 1;
            if (addr == none_addr) {
                // popEmpty - don't call lastCompiled
            } else {
                try unfinished.lastCompiled(self.alloc, addr);
            }
            addr = try self.compile(&unfinished.node);
            unfinished.node.deinit(self.alloc);
        }
        try self.stack.items[self.stack.items.len - 1].lastCompiled(self.alloc, addr);
    }

    fn compile(self: *Builder, node: *const BuilderNode) !usize {
        if (node.final and node.trans.items.len == 0 and node.final_output == 0) {
            return 0;
        }

        const reg_result = self.registry.entry(node);
        if (reg_result.found) {
            return reg_result.addr;
        }

        const addr = try self.encodeState(node);

        // Store node copy in registry
        if (reg_result.cell_idx) |cell_idx| {
            var copy = BuilderNode{
                .final = node.final,
                .final_output = node.final_output,
            };
            try copy.trans.appendSlice(self.alloc, node.trans.items);
            self.registry.table[cell_idx].node = copy;
            self.registry.table[cell_idx].addr = addr;
            self.registry.table[cell_idx].gen = self.registry.gen;
        }

        self.last_addr = addr;
        return addr;
    }

    fn encodeState(self: *Builder, s: *const BuilderNode) !usize {
        if (s.trans.items.len == 0 and s.final and s.final_output == 0) {
            return 0;
        } else if (s.trans.items.len != 1 or s.final) {
            return self.encodeStateMany(s);
        } else if (!s.final and s.trans.items[0].out == 0 and s.trans.items[0].addr == self.last_addr) {
            return self.encodeStateOneFinish(s, transition_next);
        }
        return self.encodeStateOne(s);
    }

    fn encodeStateOne(self: *Builder, s: *const BuilderNode) !usize {
        const start: u64 = @intCast(self.out.items.len);
        var out_pack_size: u8 = 0;
        if (s.trans.items[0].out != 0) {
            out_pack_size = packedSize(s.trans.items[0].out);
            try self.writePackedUintIn(s.trans.items[0].out, out_pack_size);
        }
        const delta = deltaAddr(start, @intCast(s.trans.items[0].addr));
        const trans_pack_size = packedSize(delta);
        try self.writePackedUintIn(delta, trans_pack_size);

        const pack_size_byte = encodePackSize(trans_pack_size, out_pack_size);
        try self.out.append(self.alloc, pack_size_byte);

        return self.encodeStateOneFinish(s, 0);
    }

    fn encodeStateOneFinish(self: *Builder, s: *const BuilderNode, next_flag: u8) !usize {
        const enc = encodeCommon(s.trans.items[0].in);
        if (enc == 0) {
            try self.out.append(self.alloc, s.trans.items[0].in);
        }
        try self.out.append(self.alloc, one_transition | next_flag | enc);
        return self.out.items.len - 1;
    }

    fn encodeStateMany(self: *Builder, s: *const BuilderNode) !usize {
        const start: u64 = @intCast(self.out.items.len);
        var trans_pack_size: u8 = 0;
        var out_pack_size: u8 = packedSize(s.final_output);
        var any_outputs = s.final_output != 0;

        for (s.trans.items) |t| {
            const delta = deltaAddr(start, @intCast(t.addr));
            const tsize = packedSize(delta);
            if (tsize > trans_pack_size) trans_pack_size = tsize;
            const osize = packedSize(t.out);
            if (osize > out_pack_size) out_pack_size = osize;
            any_outputs = any_outputs or t.out != 0;
        }
        if (!any_outputs) out_pack_size = 0;

        if (any_outputs) {
            // Write final output
            if (s.final) {
                try self.writePackedUintIn(s.final_output, out_pack_size);
            }
            // Write transition outputs in reverse
            var j: usize = s.trans.items.len;
            while (j > 0) {
                j -= 1;
                try self.writePackedUintIn(s.trans.items[j].out, out_pack_size);
            }
        }

        // Write transition destinations in reverse
        {
            var j: usize = s.trans.items.len;
            while (j > 0) {
                j -= 1;
                const delta = deltaAddr(start, @intCast(s.trans.items[j].addr));
                try self.writePackedUintIn(delta, trans_pack_size);
            }
        }

        // Write transition keys in reverse
        {
            var j: usize = s.trans.items.len;
            while (j > 0) {
                j -= 1;
                try self.out.append(self.alloc, s.trans.items[j].in);
            }
        }

        // Pack size byte
        try self.out.append(self.alloc, encodePackSize(trans_pack_size, out_pack_size));

        // Number of transitions
        const num_trans = s.trans.items.len;
        const encoded_num: u8 = if (num_trans <= max_num_trans) @intCast(num_trans) else 0;

        if (encoded_num == 0) {
            if (num_trans == 256) {
                try self.out.append(self.alloc, 1); // special: 1 means 256
            } else {
                try self.out.append(self.alloc, @intCast(num_trans));
            }
        }

        // Header byte
        var header: u8 = encoded_num;
        if (s.final) header |= state_final;
        try self.out.append(self.alloc, header);

        return self.out.items.len - 1;
    }

    fn writePackedUintIn(self: *Builder, v: u64, n: u8) !void {
        var shift: u6 = 0;
        for (0..n) |_| {
            try self.out.append(self.alloc, @truncate(v >> shift));
            shift +%= 8;
        }
    }
};

// ============================================================================
// FST Reader / Decoder
// ============================================================================

/// Decoded state from the FST byte stream.
const FstState = struct {
    data: []const u8,
    top: usize = 0,
    bottom: usize = 0,
    num_trans: usize = 0,

    // single transition
    single_trans_char: u8 = 0,
    single_trans_next: bool = false,
    single_trans_addr: u64 = 0,
    single_trans_out: u64 = 0,

    // shared
    trans_size: u8 = 0,
    out_size: u8 = 0,

    // multi transition
    is_final: bool = false,
    trans_top: usize = 0,
    trans_bottom: usize = 0,
    dest_top: usize = 0,
    dest_bottom: usize = 0,
    out_top: usize = 0,
    out_bottom: usize = 0,
    out_final: usize = 0,

    is_single: bool = false,

    fn at(data: []const u8, addr: usize) !FstState {
        if (addr == empty_addr) return atZero(data);
        if (addr == none_addr) return atNone(data);
        if (addr >= data.len or addr < header_size) return error.InvalidAddress;

        var s = FstState{ .data = data, .top = addr, .bottom = addr };
        if (data[s.top] >> 7 > 0) {
            s.is_single = true;
            try s.atSingle();
        } else {
            try s.atMulti();
        }
        return s;
    }

    fn atZero(data: []const u8) FstState {
        return .{ .data = data, .top = 0, .bottom = 1, .num_trans = 0, .is_final = true, .out_final = 0 };
    }

    fn atNone(data: []const u8) FstState {
        return .{ .data = data, .top = 0, .bottom = 1, .num_trans = 0, .is_final = false };
    }

    fn atSingle(self: *FstState) !void {
        self.num_trans = 1;
        self.single_trans_next = (self.data[self.top] & transition_next) > 0;
        self.single_trans_char = self.data[self.top] & max_common;
        if (self.single_trans_char == 0) {
            self.bottom -= 1;
            self.single_trans_char = self.data[self.bottom];
        } else {
            self.single_trans_char = decodeCommon(self.single_trans_char);
        }

        if (self.single_trans_next) {
            self.single_trans_addr = @intCast(self.bottom - 1);
            self.single_trans_out = 0;
        } else {
            self.bottom -= 1;
            const pack = self.data[self.bottom];
            self.trans_size = decodePackSizeTrans(pack);
            self.out_size = decodePackSizeOut(pack);

            self.bottom -= self.trans_size;
            self.single_trans_addr = readPackedUint(self.data[self.bottom .. self.bottom + self.trans_size]);

            if (self.out_size > 0) {
                self.bottom -= self.out_size;
                self.single_trans_out = readPackedUint(self.data[self.bottom .. self.bottom + self.out_size]);
            } else {
                self.single_trans_out = 0;
            }

            // Convert delta to absolute address
            if (self.single_trans_addr != 0) {
                self.single_trans_addr = @as(u64, @intCast(self.bottom)) - self.single_trans_addr;
            }
        }
    }

    fn atMulti(self: *FstState) !void {
        self.is_final = (self.data[self.top] & state_final) > 0;
        self.num_trans = @intCast(self.data[self.top] & max_num_trans);
        if (self.num_trans == 0) {
            self.bottom -= 1;
            self.num_trans = @intCast(self.data[self.bottom]);
            if (self.num_trans == 1) {
                // Special case: 1 encoded here means 256
                self.num_trans = 256;
            }
        }
        self.bottom -= 1;
        const pack = self.data[self.bottom];
        self.trans_size = decodePackSizeTrans(pack);
        self.out_size = decodePackSizeOut(pack);

        self.trans_top = self.bottom;
        self.bottom -= self.num_trans;
        self.trans_bottom = self.bottom;

        self.dest_top = self.bottom;
        self.bottom -= self.num_trans * self.trans_size;
        self.dest_bottom = self.bottom;

        if (self.out_size > 0) {
            self.out_top = self.bottom;
            self.bottom -= self.num_trans * self.out_size;
            self.out_bottom = self.bottom;
            if (self.is_final) {
                self.bottom -= self.out_size;
                self.out_final = self.bottom;
            }
        }
    }

    fn final(self: *const FstState) bool {
        return self.is_final;
    }

    fn finalOutput(self: *const FstState) u64 {
        if (self.is_final and self.out_size > 0) {
            return readPackedUint(self.data[self.out_final .. self.out_final + self.out_size]);
        }
        return 0;
    }

    fn numTransitions(self: *const FstState) usize {
        return self.num_trans;
    }

    fn transitionAt(self: *const FstState, i: usize) u8 {
        if (self.is_single) return self.single_trans_char;
        const keys = self.data[self.trans_bottom..self.trans_top];
        return keys[self.num_trans - i - 1];
    }

    /// Returns (position, next_addr, output) for the given byte.
    /// position = -1 (as maxInt) if not found, next_addr = none_addr.
    fn transitionFor(self: *const FstState, b: u8) struct { pos: usize, addr: usize, out: u64 } {
        if (self.is_single) {
            if (self.single_trans_char == b) {
                return .{ .pos = 0, .addr = @intCast(self.single_trans_addr), .out = self.single_trans_out };
            }
            return .{ .pos = std.math.maxInt(usize), .addr = none_addr, .out = 0 };
        }

        const keys = self.data[self.trans_bottom..self.trans_top];
        var found_pos: ?usize = null;
        for (keys, 0..) |k, idx| {
            if (k == b) {
                found_pos = idx;
                break;
            }
        }
        if (found_pos == null) {
            return .{ .pos = std.math.maxInt(usize), .addr = none_addr, .out = 0 };
        }
        const pos = found_pos.?;

        const dests = self.data[self.dest_bottom..self.dest_top];
        var dest = @as(usize, @intCast(readPackedUint(dests[pos * self.trans_size .. pos * self.trans_size + self.trans_size])));
        if (dest > 0) {
            dest = self.bottom - dest; // convert delta
        }

        var out: u64 = 0;
        if (self.out_size > 0) {
            const vals = self.data[self.out_bottom..self.out_top];
            out = readPackedUint(vals[pos * self.out_size .. pos * self.out_size + self.out_size]);
        }

        return .{ .pos = self.num_trans - pos - 1, .addr = dest, .out = out };
    }
};

// ============================================================================
// Automaton interface
// ============================================================================

/// Generic automaton for FST traversal filtering.
pub const Automaton = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    const VTable = struct {
        start: *const fn (*anyopaque) usize,
        isMatch: *const fn (*anyopaque, usize) bool,
        canMatch: *const fn (*anyopaque, usize) bool,
        willAlwaysMatch: *const fn (*anyopaque, usize) bool,
        accept: *const fn (*anyopaque, usize, u8) usize,
    };

    pub fn start(self: Automaton) usize {
        return self.vtable.start(self.ptr);
    }

    pub fn isMatch(self: Automaton, state: usize) bool {
        return self.vtable.isMatch(self.ptr, state);
    }

    pub fn canMatch(self: Automaton, state: usize) bool {
        return self.vtable.canMatch(self.ptr, state);
    }

    pub fn willAlwaysMatch(self: Automaton, state: usize) bool {
        return self.vtable.willAlwaysMatch(self.ptr, state);
    }

    pub fn accept(self: Automaton, state: usize, b: u8) usize {
        return self.vtable.accept(self.ptr, state, b);
    }
};

/// An automaton that matches everything.
pub const AlwaysMatch = struct {
    pub fn automaton(self: *AlwaysMatch) Automaton {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .start = @ptrCast(&startFn),
                .isMatch = @ptrCast(&isMatchFn),
                .canMatch = @ptrCast(&canMatchFn),
                .willAlwaysMatch = @ptrCast(&willAlwaysMatchFn),
                .accept = @ptrCast(&acceptFn),
            },
        };
    }

    fn startFn(_: *AlwaysMatch) usize {
        return 0;
    }
    fn isMatchFn(_: *AlwaysMatch, _: usize) bool {
        return true;
    }
    fn canMatchFn(_: *AlwaysMatch, _: usize) bool {
        return true;
    }
    fn willAlwaysMatchFn(_: *AlwaysMatch, _: usize) bool {
        return true;
    }
    fn acceptFn(_: *AlwaysMatch, _: usize, _: u8) usize {
        return 0;
    }
};

/// An automaton that matches keys starting with a given prefix.
pub const StartsWith = struct {
    prefix: []const u8,

    pub fn automaton(self: *StartsWith) Automaton {
        return .{
            .ptr = @ptrCast(self),
            .vtable = &.{
                .start = @ptrCast(&startFn),
                .isMatch = @ptrCast(&isMatchFn),
                .canMatch = @ptrCast(&canMatchFn),
                .willAlwaysMatch = @ptrCast(&willAlwaysMatchFn),
                .accept = @ptrCast(&acceptFn),
            },
        };
    }

    fn startFn(_: *StartsWith) usize {
        return 0;
    }

    fn isMatchFn(self: *StartsWith, state: usize) bool {
        return state >= self.prefix.len;
    }

    fn canMatchFn(_: *StartsWith, state: usize) bool {
        return state != std.math.maxInt(usize);
    }

    fn willAlwaysMatchFn(self: *StartsWith, state: usize) bool {
        return state >= self.prefix.len;
    }

    fn acceptFn(self: *StartsWith, state: usize, b: u8) usize {
        if (state >= self.prefix.len) return state; // already matched prefix
        if (b == self.prefix[state]) return state + 1;
        return std.math.maxInt(usize); // dead state
    }
};

// ============================================================================
// FST (read-only loaded FST)
// ============================================================================

/// A loaded, read-only FST supporting exact lookup, range iteration,
/// and automaton-based search.
pub const FST = struct {
    data: []const u8,
    root_addr: usize,
    count: usize,

    pub fn load(data: []const u8) !FST {
        if (data.len < header_size + footer_size) return error.InvalidFST;

        // Verify header
        const ver = std.mem.readInt(u64, data[0..8], .little);
        if (ver != version_v1) return error.UnsupportedVersion;

        // Read footer
        const footer = data[data.len - footer_size ..];
        const count = std.mem.readInt(u64, footer[0..8], .little);
        const root_addr = std.mem.readInt(u64, footer[8..16], .little);

        return .{
            .data = data,
            .root_addr = @intCast(root_addr),
            .count = @intCast(count),
        };
    }

    /// Look up the value for a key. Returns (value, found).
    pub fn get(self: *const FST, input: []const u8) !struct { val: u64, found: bool } {
        var total: u64 = 0;
        var state = try FstState.at(self.data, self.root_addr);

        for (input) |c| {
            const result = state.transitionFor(c);
            if (result.addr == none_addr) return .{ .val = 0, .found = false };
            state = try FstState.at(self.data, result.addr);
            total += result.out;
        }

        if (state.final()) {
            total += state.finalOutput();
            return .{ .val = total, .found = true };
        }
        return .{ .val = 0, .found = false };
    }

    /// Check if the FST contains a key.
    pub fn contains(self: *const FST, key: []const u8) !bool {
        const result = try self.get(key);
        return result.found;
    }

    /// Return the number of entries.
    pub fn len(self: *const FST) usize {
        return self.count;
    }

    /// Create an iterator over all keys in [start, end).
    pub fn iterator(self: *const FST, alloc: Allocator, start_inclusive: ?[]const u8, end_exclusive: ?[]const u8) !FSTIterator {
        return FSTIterator.init(alloc, self, start_inclusive, end_exclusive, null);
    }

    /// Create an iterator with automaton filtering.
    pub fn search(self: *const FST, alloc: Allocator, aut: Automaton, start_inclusive: ?[]const u8, end_exclusive: ?[]const u8) !FSTIterator {
        return FSTIterator.init(alloc, self, start_inclusive, end_exclusive, aut);
    }

    /// Get the minimum key in the FST.
    pub fn getMinKey(self: *const FST, alloc: Allocator) ![]u8 {
        var result = std.ArrayListUnmanaged(u8).empty;
        defer result.deinit(alloc);
        var state = try FstState.at(self.data, self.root_addr);
        while (!state.final()) {
            if (state.numTransitions() == 0) break;
            const t = state.transitionAt(0);
            const r = state.transitionFor(t);
            state = try FstState.at(self.data, r.addr);
            try result.append(alloc, t);
        }
        return try alloc.dupe(u8, result.items);
    }

    /// Get the maximum key in the FST.
    pub fn getMaxKey(self: *const FST, alloc: Allocator) ![]u8 {
        var result = std.ArrayListUnmanaged(u8).empty;
        defer result.deinit(alloc);
        var state = try FstState.at(self.data, self.root_addr);
        while (state.numTransitions() > 0) {
            const t = state.transitionAt(state.numTransitions() - 1);
            const r = state.transitionFor(t);
            state = try FstState.at(self.data, r.addr);
            try result.append(alloc, t);
        }
        return try alloc.dupe(u8, result.items);
    }
};

// ============================================================================
// FST Iterator
// ============================================================================

pub const FSTIterator = struct {
    pub const Entry = struct { key: []const u8, val: u64 };

    alloc: Allocator,
    fst: *const FST,
    aut: ?Automaton,
    start_inclusive: ?[]const u8,
    end_exclusive: ?[]const u8,

    states_stack: std.ArrayListUnmanaged(FstState),
    keys_stack: std.ArrayListUnmanaged(u8),
    keys_pos_stack: std.ArrayListUnmanaged(usize),
    vals_stack: std.ArrayListUnmanaged(u64),
    aut_states_stack: std.ArrayListUnmanaged(usize),

    next_start: std.ArrayListUnmanaged(u8),
    done: bool = false,

    fn init(alloc: Allocator, fst: *const FST, start_inclusive: ?[]const u8, end_exclusive: ?[]const u8, aut: ?Automaton) !FSTIterator {
        var it = FSTIterator{
            .alloc = alloc,
            .fst = fst,
            .aut = aut,
            .start_inclusive = start_inclusive,
            .end_exclusive = end_exclusive,
            .states_stack = .empty,
            .keys_stack = .empty,
            .keys_pos_stack = .empty,
            .vals_stack = .empty,
            .aut_states_stack = .empty,
            .next_start = .empty,
        };
        try it.pointTo(start_inclusive);
        return it;
    }

    pub fn deinit(self: *FSTIterator) void {
        self.states_stack.deinit(self.alloc);
        self.keys_stack.deinit(self.alloc);
        self.keys_pos_stack.deinit(self.alloc);
        self.vals_stack.deinit(self.alloc);
        self.aut_states_stack.deinit(self.alloc);
        self.next_start.deinit(self.alloc);
    }

    fn pointTo(self: *FSTIterator, key: ?[]const u8) !void {
        var seek_key = key;

        // Clamp to range
        if (seek_key != null and self.start_inclusive != null) {
            if (std.mem.order(u8, seek_key.?, self.start_inclusive.?) == .lt) {
                seek_key = self.start_inclusive;
            }
        }
        if (seek_key != null and self.end_exclusive != null) {
            if (std.mem.order(u8, seek_key.?, self.end_exclusive.?) == .gt) {
                seek_key = self.end_exclusive;
            }
        }

        // Reset stacks
        self.states_stack.clearRetainingCapacity();
        self.keys_stack.clearRetainingCapacity();
        self.keys_pos_stack.clearRetainingCapacity();
        self.vals_stack.clearRetainingCapacity();
        self.aut_states_stack.clearRetainingCapacity();

        const root = try FstState.at(self.fst.data, self.fst.root_addr);
        const aut_start: usize = if (self.aut) |a| a.start() else 0;

        var max_q: isize = -1;
        try self.states_stack.append(self.alloc, root);
        try self.aut_states_stack.append(self.alloc, aut_start);

        if (seek_key) |sk| {
            for (sk) |key_j| {
                const curr = self.states_stack.items[self.states_stack.items.len - 1];
                const aut_curr = self.aut_states_stack.items[self.aut_states_stack.items.len - 1];

                const result = curr.transitionFor(key_j);
                if (result.addr == none_addr) {
                    // Find last transition before the one we needed
                    var q: isize = @intCast(curr.numTransitions());
                    q -= 1;
                    while (q >= 0) : (q -= 1) {
                        if (curr.transitionAt(@intCast(q)) < key_j) {
                            max_q = q;
                            break;
                        }
                    }
                    break;
                }

                const aut_next: usize = if (self.aut) |a| a.accept(aut_curr, key_j) else 0;
                const next_state = try FstState.at(self.fst.data, result.addr);

                try self.states_stack.append(self.alloc, next_state);
                try self.keys_stack.append(self.alloc, key_j);
                try self.keys_pos_stack.append(self.alloc, result.pos);
                try self.vals_stack.append(self.alloc, result.out);
                try self.aut_states_stack.append(self.alloc, aut_next);
            }
        }

        const curr_state = self.states_stack.items[self.states_stack.items.len - 1];
        const curr_aut = self.aut_states_stack.items[self.aut_states_stack.items.len - 1];
        const aut_match = if (self.aut) |a| a.isMatch(curr_aut) else true;

        if (!curr_state.final() or !aut_match or
            (seek_key != null and std.mem.order(u8, self.keys_stack.items, seek_key.?) == .lt))
        {
            try self.next(max_q);
        }
    }

    /// Get current key and value.
    pub fn current(self: *const FSTIterator) ?Entry {
        if (self.done) return null;
        if (self.states_stack.items.len == 0) return null;
        const curr = self.states_stack.items[self.states_stack.items.len - 1];
        if (curr.final()) {
            var total: u64 = 0;
            for (self.vals_stack.items) |v| total += v;
            total += curr.finalOutput();
            return .{ .key = self.keys_stack.items, .val = total };
        }
        return null;
    }

    /// Advance to next key/value pair.
    pub fn nextEntry(self: *FSTIterator) !?Entry {
        try self.next(-1);
        return self.current();
    }

    /// Seek to the given key (or next key after it).
    pub fn seek(self: *FSTIterator, key: []const u8) !void {
        try self.pointTo(key);
    }

    fn next(self: *FSTIterator, last_offset: isize) !void {
        // Remember where we started
        self.next_start.clearRetainingCapacity();
        try self.next_start.appendSlice(self.alloc, self.keys_stack.items);

        var next_offset: isize = last_offset + 1;
        var allow_compare = false;

        while (true) {
            if (self.states_stack.items.len == 0) {
                self.done = true;
                return;
            }

            const curr = self.states_stack.items[self.states_stack.items.len - 1];
            const aut_curr = self.aut_states_stack.items[self.aut_states_stack.items.len - 1];
            const aut_match = if (self.aut) |a| a.isMatch(aut_curr) else true;

            if (curr.final() and aut_match and allow_compare) {
                // Check end boundary
                if (self.end_exclusive) |end| {
                    if (std.mem.order(u8, self.keys_stack.items, end) != .lt) {
                        self.done = true;
                        return;
                    }
                }

                if (std.mem.order(u8, self.keys_stack.items, self.next_start.items) == .gt) {
                    return; // found next valid key
                }
            }

            const num_trans: isize = @intCast(curr.numTransitions());

            // Try transitions from next_offset
            var found_next = false;
            while (next_offset < num_trans) {
                const t = curr.transitionAt(@intCast(next_offset));

                if (self.aut) |a| {
                    const aut_next = a.accept(aut_curr, t);
                    if (!a.canMatch(aut_next)) {
                        next_offset += 1;
                        continue;
                    }
                }

                const result = curr.transitionFor(t);
                const next_state = try FstState.at(self.fst.data, result.addr);

                const aut_next: usize = if (self.aut) |a| a.accept(aut_curr, t) else 0;

                try self.states_stack.append(self.alloc, next_state);
                try self.keys_stack.append(self.alloc, t);
                try self.keys_pos_stack.append(self.alloc, result.pos);
                try self.vals_stack.append(self.alloc, result.out);
                try self.aut_states_stack.append(self.alloc, aut_next);

                next_offset = 0;
                allow_compare = true;
                found_next = true;
                break;
            }

            if (found_next) continue;

            // Backtrack
            if (self.states_stack.items.len <= 1) break;

            // Pop with linear chain optimization
            var pop_num: usize = 1;
            {
                var j: usize = self.states_stack.items.len - 1;
                while (j > 0) : (j -= 1) {
                    if (j == 1 or self.states_stack.items[j].numTransitions() != 1) {
                        pop_num = self.states_stack.items.len - 1 - j;
                        break;
                    }
                }
            }
            if (pop_num < 1) pop_num = 1;

            next_offset = @as(isize, @intCast(self.keys_pos_stack.items[self.keys_pos_stack.items.len - pop_num])) + 1;
            allow_compare = false;

            self.states_stack.items.len -= pop_num;
            self.keys_stack.items.len -= pop_num;
            self.keys_pos_stack.items.len -= pop_num;
            self.vals_stack.items.len -= pop_num;
            self.aut_states_stack.items.len -= pop_num;
        }

        self.done = true;
    }
};

// ============================================================================
// Merge
// ============================================================================

pub const MergeFunc = *const fn (u64, u64) u64;

pub fn mergeMin(a: u64, b: u64) u64 {
    return @min(a, b);
}

pub fn mergeMax(a: u64, b: u64) u64 {
    return @max(a, b);
}

pub fn mergeSum(a: u64, b: u64) u64 {
    return a + b;
}

/// Merge multiple FSTs into a single FST, resolving duplicate keys with merge_fn.
pub fn merge(alloc: Allocator, fsts: []const FST, merge_fn: MergeFunc, opts: Builder.Options) ![]u8 {
    // Create iterators for all FSTs
    var iterators = try alloc.alloc(FSTIterator, fsts.len);
    defer {
        for (iterators) |*it| it.deinit();
        alloc.free(iterators);
    }
    for (fsts, 0..) |*fst, i| {
        iterators[i] = try fst.iterator(alloc, null, null);
    }

    var builder = try Builder.init(alloc, opts);
    defer builder.deinit();

    // Simple k-way merge
    while (true) {
        // Find minimum key across all iterators
        var min_key: ?[]const u8 = null;
        for (iterators) |*it| {
            if (it.current()) |entry| {
                if (min_key == null or std.mem.order(u8, entry.key, min_key.?) == .lt) {
                    min_key = entry.key;
                }
            }
        }
        if (min_key == null) break;

        // Collect and merge values for this key
        var merged_val: ?u64 = null;
        const key_copy = try alloc.dupe(u8, min_key.?);
        defer alloc.free(key_copy);

        for (iterators) |*it| {
            if (it.current()) |entry| {
                if (std.mem.eql(u8, entry.key, key_copy)) {
                    if (merged_val) |existing| {
                        merged_val = merge_fn(existing, entry.val);
                    } else {
                        merged_val = entry.val;
                    }
                    _ = try it.nextEntry();
                }
            }
        }

        try builder.insert(key_copy, merged_val.?);
    }

    return builder.finish();
}

// ============================================================================
// Tests
// ============================================================================

test "registry tolerates eviction with a tiny table" {
    // With a 4-bucket registry every insert past the 4th will evict an LRU
    // entry, exercising the in-build cleanup path. The result must still
    // round-trip: dedup is a *speed* optimization, not a correctness one.
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{ .registry_table_size = 4 });
    defer builder.deinit();

    const keys = [_][]const u8{ "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel" };
    for (keys, 0..) |k, i| try builder.insert(k, i);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);
    try std.testing.expectEqual(@as(usize, keys.len), fst.len());
    for (keys, 0..) |k, i| {
        const r = try fst.get(k);
        try std.testing.expect(r.found);
        try std.testing.expectEqual(@as(u64, i), r.val);
    }
}

test "registry rollover sweep frees stale nodes" {
    // Force the rare gen-rollover path so the full-table sweep runs at least
    // once under the testing allocator's leak detector.
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{ .registry_table_size = 4 });
    defer builder.deinit();

    // Seed the registry so several cells have non-zero gen (i.e. real
    // BuilderNodes that the rollover sweep will need to deinit).
    try builder.insert("aa", 1);
    try builder.insert("bb", 2);
    try builder.insert("cc", 3);
    {
        const data = try builder.finish();
        defer alloc.free(data);
    }

    // Jump the registry generation to maxInt(u32) so the next reset triggers
    // the rollover branch in Registry.reset(). This is white-box but the only
    // way to exercise it deterministically.
    builder.registry.gen = std.math.maxInt(u32);
    try builder.reset();
    try std.testing.expectEqual(@as(u32, 1), builder.registry.gen);

    try builder.insert("xx", 100);
    {
        const data = try builder.finish();
        defer alloc.free(data);
        const fst = try FST.load(data);
        try std.testing.expect((try fst.get("xx")).found);
        try std.testing.expect(!(try fst.get("aa")).found);
    }
}

test "builder.reset reuses one Builder for multiple FSTs" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    // First FST: cats / dogs.
    try builder.insert("cat", 1);
    try builder.insert("dog", 2);
    {
        const data = try builder.finish();
        defer alloc.free(data);
        const fst = try FST.load(data);
        try std.testing.expectEqual(@as(usize, 2), fst.len());
        try std.testing.expectEqual(@as(u64, 1), (try fst.get("cat")).val);
        try std.testing.expectEqual(@as(u64, 2), (try fst.get("dog")).val);
    }

    // After reset the builder must look fresh: previously-inserted keys must
    // not be retrievable, and a brand-new key set must build cleanly.
    try builder.reset();
    try builder.insert("ant", 10);
    try builder.insert("bee", 20);
    try builder.insert("fox", 30);
    {
        const data = try builder.finish();
        defer alloc.free(data);
        const fst = try FST.load(data);
        try std.testing.expectEqual(@as(usize, 3), fst.len());
        try std.testing.expectEqual(@as(u64, 10), (try fst.get("ant")).val);
        try std.testing.expectEqual(@as(u64, 20), (try fst.get("bee")).val);
        try std.testing.expectEqual(@as(u64, 30), (try fst.get("fox")).val);
        // Stale keys from the first build must not leak through the registry.
        try std.testing.expect(!(try fst.get("cat")).found);
        try std.testing.expect(!(try fst.get("dog")).found);
    }

    // Reset is idempotent: a second reset followed by another build also works.
    try builder.reset();
    try builder.insert("alpha", 100);
    {
        const data = try builder.finish();
        defer alloc.free(data);
        const fst = try FST.load(data);
        try std.testing.expectEqual(@as(usize, 1), fst.len());
        try std.testing.expectEqual(@as(u64, 100), (try fst.get("alpha")).val);
    }
}

test "builder and get" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("cat", 1);
    try builder.insert("cats", 2);
    try builder.insert("dog", 3);
    try builder.insert("dogs", 4);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    // Exact lookups
    const cat = try fst.get("cat");
    try std.testing.expect(cat.found);
    try std.testing.expectEqual(@as(u64, 1), cat.val);

    const cats = try fst.get("cats");
    try std.testing.expect(cats.found);
    try std.testing.expectEqual(@as(u64, 2), cats.val);

    const dog = try fst.get("dog");
    try std.testing.expect(dog.found);
    try std.testing.expectEqual(@as(u64, 3), dog.val);

    const dogs = try fst.get("dogs");
    try std.testing.expect(dogs.found);
    try std.testing.expectEqual(@as(u64, 4), dogs.val);

    // Non-existent key
    const bird = try fst.get("bird");
    try std.testing.expect(!bird.found);

    try std.testing.expectEqual(@as(usize, 4), fst.len());
}

test "contains" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("hello", 42);
    try builder.insert("world", 99);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);
    try std.testing.expect(try fst.contains("hello"));
    try std.testing.expect(try fst.contains("world"));
    try std.testing.expect(!try fst.contains("missing"));
}

test "empty key" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("", 100);
    try builder.insert("a", 200);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    const empty = try fst.get("");
    try std.testing.expect(empty.found);
    try std.testing.expectEqual(@as(u64, 100), empty.val);

    const a = try fst.get("a");
    try std.testing.expect(a.found);
    try std.testing.expectEqual(@as(u64, 200), a.val);
}

test "out of order insert" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("dog", 1);
    try std.testing.expectError(error.OutOfOrder, builder.insert("cat", 2));
}

test "iterator all keys" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("alpha", 1);
    try builder.insert("beta", 2);
    try builder.insert("gamma", 3);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    var it = try fst.iterator(alloc, null, null);
    defer it.deinit();

    // First entry
    const e1 = it.current() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("alpha", e1.key);
    try std.testing.expectEqual(@as(u64, 1), e1.val);

    // Second
    const e2 = try it.nextEntry() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("beta", e2.key);
    try std.testing.expectEqual(@as(u64, 2), e2.val);

    // Third
    const e3 = try it.nextEntry() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("gamma", e3.key);
    try std.testing.expectEqual(@as(u64, 3), e3.val);

    // Done
    const e4 = try it.nextEntry();
    try std.testing.expect(e4 == null);
}

test "iterator range" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("apple", 1);
    try builder.insert("banana", 2);
    try builder.insert("cherry", 3);
    try builder.insert("date", 4);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    // Range [banana, date) should give banana, cherry
    var it = try fst.iterator(alloc, "banana", "date");
    defer it.deinit();

    const e1 = it.current() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("banana", e1.key);

    const e2 = try it.nextEntry() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("cherry", e2.key);

    // "date" is exclusive, so done
    const e3 = try it.nextEntry();
    try std.testing.expect(e3 == null);
}

test "transition lookup handles multi-transition states" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("ant", 1);
    try builder.insert("bat", 2);
    try builder.insert("cat", 3);
    try builder.insert("dog", 4);
    try builder.insert("eel", 5);
    try builder.insert("fox", 6);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    const cat = try fst.get("cat");
    try std.testing.expect(cat.found);
    try std.testing.expectEqual(@as(u64, 3), cat.val);

    const eel = try fst.get("eel");
    try std.testing.expect(eel.found);
    try std.testing.expectEqual(@as(u64, 5), eel.val);

    const fox = try fst.get("fox");
    try std.testing.expect(fox.found);
    try std.testing.expectEqual(@as(u64, 6), fox.val);

    try std.testing.expect(!try fst.contains("gnu"));
}

test "starts with automaton" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("database", 1);
    try builder.insert("dataframe", 2);
    try builder.insert("datastore", 3);
    try builder.insert("debug", 4);
    try builder.insert("deploy", 5);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    var prefix_aut = StartsWith{ .prefix = "data" };
    var it = try fst.search(alloc, prefix_aut.automaton(), null, null);
    defer it.deinit();

    const e1 = it.current() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("database", e1.key);

    const e2 = try it.nextEntry() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("dataframe", e2.key);

    const e3 = try it.nextEntry() orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("datastore", e3.key);

    // "debug" and "deploy" should NOT match
    const e4 = try it.nextEntry();
    try std.testing.expect(e4 == null);
}

test "shared suffix deduplication" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    // These share the suffix "ing"
    try builder.insert("coding", 1);
    try builder.insert("doing", 2);
    try builder.insert("going", 3);
    try builder.insert("loving", 4);
    try builder.insert("running", 5);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);
    try std.testing.expectEqual(@as(usize, 5), fst.len());

    // All values should be correct
    for ([_]struct { key: []const u8, val: u64 }{
        .{ .key = "coding", .val = 1 },
        .{ .key = "doing", .val = 2 },
        .{ .key = "going", .val = 3 },
        .{ .key = "loving", .val = 4 },
        .{ .key = "running", .val = 5 },
    }) |tc| {
        const result = try fst.get(tc.key);
        try std.testing.expect(result.found);
        try std.testing.expectEqual(tc.val, result.val);
    }
}

test "large FST" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    // Insert 1000 keys
    var keys: [1000][16]u8 = undefined;
    for (0..1000) |i| {
        const key = std.fmt.bufPrint(&keys[i], "key_{d:0>10}", .{i}) catch unreachable;
        try builder.insert(key, @intCast(i));
    }

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);
    try std.testing.expectEqual(@as(usize, 1000), fst.len());

    // Spot-check
    const r0 = try fst.get("key_0000000000");
    try std.testing.expect(r0.found);
    try std.testing.expectEqual(@as(u64, 0), r0.val);

    const r500 = try fst.get("key_0000000500");
    try std.testing.expect(r500.found);
    try std.testing.expectEqual(@as(u64, 500), r500.val);

    const r999 = try fst.get("key_0000000999");
    try std.testing.expect(r999.found);
    try std.testing.expectEqual(@as(u64, 999), r999.val);

    try std.testing.expect(!try fst.contains("key_0000001000"));
}

test "min and max keys" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("aardvark", 1);
    try builder.insert("middle", 2);
    try builder.insert("zebra", 3);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    const min_key = try fst.getMinKey(alloc);
    defer alloc.free(min_key);
    try std.testing.expectEqualStrings("aardvark", min_key);

    const max_key = try fst.getMaxKey(alloc);
    defer alloc.free(max_key);
    try std.testing.expectEqualStrings("zebra", max_key);
}

test "zero values" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("a", 0);
    try builder.insert("b", 0);
    try builder.insert("c", 0);

    const data = try builder.finish();
    defer alloc.free(data);

    const fst = try FST.load(data);

    const a = try fst.get("a");
    try std.testing.expect(a.found);
    try std.testing.expectEqual(@as(u64, 0), a.val);
}

test "merge FSTs" {
    const alloc = std.testing.allocator;

    // Build FST 1
    var b1 = try Builder.init(alloc, .{});
    defer b1.deinit();
    try b1.insert("alpha", 10);
    try b1.insert("beta", 20);
    const data1 = try b1.finish();
    defer alloc.free(data1);
    const fst1 = try FST.load(data1);

    // Build FST 2
    var b2 = try Builder.init(alloc, .{});
    defer b2.deinit();
    try b2.insert("beta", 30);
    try b2.insert("gamma", 40);
    const data2 = try b2.finish();
    defer alloc.free(data2);
    const fst2 = try FST.load(data2);

    // Merge with sum
    const merged_data = try merge(alloc, &.{ fst1, fst2 }, &mergeSum, .{});
    defer alloc.free(merged_data);

    const merged = try FST.load(merged_data);
    try std.testing.expectEqual(@as(usize, 3), merged.len());

    const alpha = try merged.get("alpha");
    try std.testing.expect(alpha.found);
    try std.testing.expectEqual(@as(u64, 10), alpha.val);

    const beta = try merged.get("beta");
    try std.testing.expect(beta.found);
    try std.testing.expectEqual(@as(u64, 50), beta.val); // 20 + 30

    const gamma = try merged.get("gamma");
    try std.testing.expect(gamma.found);
    try std.testing.expectEqual(@as(u64, 40), gamma.val);
}

test "common input encoding roundtrip" {
    // Verify the top common bytes roundtrip correctly
    const common_bytes = [_]u8{ 't', 'e', '/', 'o', 'a', 's', 'r', 'i', 'p', 'c' };
    for (common_bytes) |b| {
        const enc = encodeCommon(b);
        try std.testing.expect(enc != 0);
        const dec = decodeCommon(enc);
        try std.testing.expectEqual(b, dec);
    }
}

test "wire compatibility: read Go-built FST" {
    // This FST was generated by Go's github.com/blevesearch/vellum:
    //   b.Insert([]byte("cat"), 1)
    //   b.Insert([]byte("cats"), 2)
    //   b.Insert([]byte("dog"), 3)
    const go_fst_hex = "01000000000000000000000000000000000100731141c1c5001097c4030101056463110203000000000000002300000000000000";
    var go_data: [go_fst_hex.len / 2]u8 = undefined;
    _ = std.fmt.hexToBytes(&go_data, go_fst_hex) catch unreachable;

    const fst = try FST.load(&go_data);
    try std.testing.expectEqual(@as(usize, 3), fst.len());

    const cat = try fst.get("cat");
    try std.testing.expect(cat.found);
    try std.testing.expectEqual(@as(u64, 1), cat.val);

    const cats = try fst.get("cats");
    try std.testing.expect(cats.found);
    try std.testing.expectEqual(@as(u64, 2), cats.val);

    const dog = try fst.get("dog");
    try std.testing.expect(dog.found);
    try std.testing.expectEqual(@as(u64, 3), dog.val);

    try std.testing.expect(!(try fst.contains("ca")));
    try std.testing.expect(!(try fst.contains("dogs")));
}

test "wire compatibility: Zig-built FST byte-identical to Go" {
    const alloc = std.testing.allocator;

    var builder = try Builder.init(alloc, .{});
    defer builder.deinit();

    try builder.insert("cat", 1);
    try builder.insert("cats", 2);
    try builder.insert("dog", 3);

    const data = try builder.finish();
    defer alloc.free(data);

    // Expected output from Go vellum with same inputs
    const go_fst_hex = "01000000000000000000000000000000000100731141c1c5001097c4030101056463110203000000000000002300000000000000";
    var expected: [go_fst_hex.len / 2]u8 = undefined;
    _ = std.fmt.hexToBytes(&expected, go_fst_hex) catch unreachable;

    try std.testing.expectEqualSlices(u8, &expected, data);
}
