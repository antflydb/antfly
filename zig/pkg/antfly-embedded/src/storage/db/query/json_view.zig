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

const std = @import("std");
const Allocator = std.mem.Allocator;
const Value = std.json.Value;
const plan_mod = @import("json_projection_plan.zig");
pub const Selector = plan_mod.Selector;

/// Arena-owned, lazy navigation over pinned JSON bytes. Only visited containers
/// acquire an index; unselected subtrees remain raw spans. Scanner.skipValue
/// validates their syntax without allocating their strings or DOM. A view may
/// be reused across predicates and projections while its input stays pinned.
pub const View = struct {
    alloc: Allocator,
    root: Node,
    materialized_values: usize = 0,
    parse_numbers: bool = false,
    root_sources: []const RootSource = &.{},
    pub const RootSource = struct {
        name: []const u8,
        value: Value = .null,
        embeddings: ?*const @import("embedding_fields.zig").EmbeddingFields = null,
        embedding_stream: ?*@import("embedding_fields.zig").EmbeddingStream = null,
        pub fn outputSource(self: *const @This()) @import("projection_output.zig").Source {
            if (self.embedding_stream) |stream| return .{ .embedding_stream = stream };
            return if (self.embeddings) |fields| .{ .embeddings = fields } else .{ .logical = &self.value };
        }
        fn include(self: @This(), alloc: Allocator, dst: *std.json.ObjectMap, path: []const []const u8) !void {
            var source = std.json.ObjectMap.empty;
            try source.put(alloc, self.name, if (self.embedding_stream) |stream| try (try stream.fields(alloc)).materialize(alloc) else if (self.embeddings) |fields| try fields.materialize(alloc) else self.value);
            try @import("../document_query.zig").applyIncludeBorrowedPath(alloc, source, dst, path);
        }
    };

    /// Small prepared objects use dense selector slots, not a minimum-size
    /// hash table per object. Wide and lazily discovered objects keep hashing.
    pub const ObjectIndex = struct {
        const Hash = std.StringHashMapUnmanaged(usize);
        const Entry = struct { name: []const u8, index: usize = std.math.maxInt(usize) };
        storage: union(enum) { map: Hash, compact: []Entry } = .{ .map = .empty },
        pub const empty: ObjectIndex = .{};
        fn selected(a: Allocator, selector: *const Selector) !ObjectIndex {
            const entries = try a.alloc(Entry, selector.fields.count());
            var it = selector.fields.iterator();
            for (entries) |*entry| entry.* = .{ .name = try a.dupe(u8, it.next().?.key_ptr.*) };
            return .{ .storage = .{ .compact = entries } };
        }
        fn canonicalName(self: *const @This(), name: []const u8) ?[]const u8 {
            return switch (self.storage) {
                .map => null,
                .compact => |entries| for (entries) |entry| {
                    if (std.mem.eql(u8, entry.name, name)) break entry.name;
                } else null,
            };
        }
        pub fn get(self: *const @This(), name: []const u8) ?usize {
            return switch (self.storage) {
                .map => |map| map.get(name),
                .compact => |entries| for (entries) |entry| {
                    if (std.mem.eql(u8, entry.name, name)) break entry.index;
                } else null,
            };
        }
        pub fn contains(self: *const @This(), name: []const u8) bool {
            return self.get(name) != null;
        }
        pub fn count(self: *const @This()) u32 {
            return switch (self.storage) {
                .map => |map| map.count(),
                .compact => |entries| @intCast(entries.len),
            };
        }
        fn clearRetainingCapacity(self: *@This()) void {
            switch (self.storage) {
                .map => |*map| map.clearRetainingCapacity(),
                .compact => |entries| for (entries) |*entry| {
                    entry.index = std.math.maxInt(usize);
                },
            }
        }
        fn put(self: *@This(), a: Allocator, name: []const u8, index: usize) !void {
            switch (self.storage) {
                .map => |*map| try map.put(a, name, index),
                .compact => |entries| {
                    for (entries) |*entry| if (std.mem.eql(u8, entry.name, name)) {
                        entry.index = index;
                        return;
                    };
                    // Preserve general insertion behavior for callers outside
                    // preparation; publish promotion only after it succeeds.
                    var map: Hash = .empty;
                    for (entries) |entry| try map.put(a, entry.name, entry.index);
                    try map.put(a, name, index);
                    self.storage = .{ .map = map };
                },
            }
        }
        const Iterator = union(enum) {
            map: Hash.Iterator,
            compact: struct { entries: []Entry, offset: usize = 0 },
            const Result = struct { key_ptr: *const []const u8, value_ptr: *usize };
            pub fn next(self: *@This()) ?Result {
                switch (self.*) {
                    .map => |*it| {
                        const e = it.next() orelse return null;
                        return .{ .key_ptr = e.key_ptr, .value_ptr = e.value_ptr };
                    },
                    .compact => |*it| {
                        if (it.offset == it.entries.len) return null;
                        const e = &it.entries[it.offset];
                        it.offset += 1;
                        return .{ .key_ptr = &e.name, .value_ptr = &e.index };
                    },
                }
            }
        };
        pub fn iterator(self: *const @This()) Iterator {
            return switch (self.storage) {
                .map => |*map| .{ .map = map.iterator() },
                .compact => |entries| .{ .compact = .{ .entries = entries } },
            };
        }
    };

    pub const Node = struct {
        raw: []const u8,
        children: ?[]Child = null,
        object_index: ObjectIndex = .empty,
        array_index: std.AutoHashMapUnmanaged(usize, ?*Node) = .empty,
        // Keep numeric predicate caches inline without reserving a full Value
        // in every span. Strings use compact descriptors; containers use Values.
        logical: union(enum) {
            empty,
            null,
            bool: bool,
            integer: i64,
            float: f64,
            string: *[]const u8,
            number_string: *[]const u8,
            container: *Value,
        } = .empty,
        state: packed struct(u8) {
            object_complete: bool = false,
            // Every object item has had its decoded sibling keys checked.
            // This proof avoids repeating key-table work during lazy emission.
            fanout_keys_validated: bool = false,
            reserved: u6 = 0,
        } = .{},

        fn objectComplete(self: Node) bool {
            return self.state.object_complete;
        }
        fn markObjectComplete(self: *Node, complete: bool) void {
            self.state.object_complete = complete;
        }

        pub fn kind(self: Node) u8 {
            const raw = std.mem.trim(u8, self.raw, " \r\n\t");
            return if (raw.len == 0) 0 else raw[0];
        }
    };
    pub const Child = struct { node: Node };

    pub fn init(alloc: Allocator, raw: []const u8) View {
        return .{ .alloc = alloc, .root = .{ .raw = raw } };
    }

    /// Seed one member discovered by a streaming object scan. Both the name
    /// and navigation belong to the item view, independent of selector lifetime.
    pub fn initProjectedObject(alloc: Allocator, raw: []const u8, name: []const u8, selected: ?[]const u8) !View {
        var view = View.init(alloc, raw);
        const entries = try alloc.alloc(ObjectIndex.Entry, 1);
        entries[0] = .{ .name = try alloc.dupe(u8, name), .index = if (selected != null) 0 else std.math.maxInt(usize) };
        view.root.object_index = .{ .storage = .{ .compact = entries } };
        if (selected) |value| {
            const slots = try alloc.alloc(Child, 1);
            slots[0] = .{ .node = .{ .raw = value } };
            view.root.children = slots;
        }
        return view;
    }

    pub fn children(self: *View, node: *Node) ![]Child {
        const object = node.kind() == '{';
        if (node.children) |cached| if (!object or node.objectComplete()) return cached;
        if (!object and node.kind() != '[') return &.{};
        var scanner = std.json.Scanner.initCompleteInput(self.alloc, node.raw);
        defer scanner.deinit();
        _ = try scanner.next();
        var entries = std.ArrayListUnmanaged(Child).empty;
        var index = std.StringHashMapUnmanaged(usize).empty;
        while (true) {
            const token = try scanner.peekNextTokenType();
            if (token == .object_end or token == .array_end) break;
            var name: []const u8 = "";
            if (object) {
                name = switch (try scanner.nextAllocMax(self.alloc, .alloc_if_needed, node.raw.len)) {
                    .string, .allocated_string => |key| key,
                    else => return error.SyntaxError,
                };
            }
            switch (try scanner.peekNextTokenType()) {
                .object_end, .array_end, .end_of_document => return error.SyntaxError,
                else => {},
            }
            const start = scanner.cursor;
            try scanner.skipValue();
            if (object) {
                const entry = try index.getOrPut(self.alloc, name);
                if (entry.found_existing) return error.DuplicateField;
                entry.value_ptr.* = entries.items.len;
            }
            try entries.append(self.alloc, .{ .node = .{ .raw = node.raw[start..scanner.cursor] } });
        }
        _ = try scanner.next();
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
        node.children = try entries.toOwnedSlice(self.alloc);
        node.object_index = .{ .storage = .{ .map = index } };
        node.markObjectComplete(object);
        return node.children.?;
    }

    pub fn field(self: *View, node: *Node, name: []const u8) !?*Node {
        if (node.kind() != '{') return null;
        if (node.object_index.get(name)) |i| {
            if (i == std.math.maxInt(usize)) return null;
            if (node.children) |entries| if (i < entries.len) return &entries[i].node;
        }
        if (node.objectComplete()) return null;
        // An unplanned field still has the ordinary lazy behavior. A partial
        // projection cache is never a complete index of the original object.
        const entries = try self.children(node);
        const i = node.object_index.get(name) orelse return null;
        return &entries[i].node;
    }

    /// Output-only lookup: retain the requested member rather than indexing
    /// every sibling of a streaming fanout item. Scan all sibling keys to keep
    /// duplicate and syntax validation identical to ordinary lazy navigation.
    pub fn fieldSparse(self: *View, node: *Node, name: []const u8) !?*Node {
        if (node.kind() != '{') return null;
        if (node.object_index.get(name)) |i| {
            if (i == std.math.maxInt(usize)) return null;
            if (node.children) |entries| if (i < entries.len) return &entries[i].node;
        }
        if (node.objectComplete()) return null;
        var scanner = std.json.Scanner.initCompleteInput(self.alloc, node.raw);
        defer scanner.deinit();
        _ = try scanner.next();
        var names: std.StringHashMapUnmanaged(void) = .empty;
        var selected: ?[]const u8 = null;
        while (try scanner.peekNextTokenType() != .object_end) {
            const key = switch (try scanner.nextAllocMax(self.alloc, .alloc_if_needed, node.raw.len)) {
                .string, .allocated_string => |key| key,
                else => return error.SyntaxError,
            };
            if ((try names.getOrPut(self.alloc, key)).found_existing) return error.DuplicateField;
            switch (try scanner.peekNextTokenType()) {
                .object_end, .array_end, .end_of_document => return error.SyntaxError,
                else => {},
            }
            const start = scanner.cursor;
            try scanner.skipValue();
            if (std.mem.eql(u8, key, name)) selected = node.raw[start..scanner.cursor];
        }
        _ = try scanner.next();
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
        const owned_name = node.object_index.canonicalName(name) orelse try self.alloc.dupe(u8, name);
        if (selected) |raw| {
            const old = node.children orelse &.{};
            const entries = try self.alloc.alloc(Child, old.len + 1);
            @memcpy(entries[0..old.len], old);
            entries[old.len] = .{ .node = .{ .raw = raw } };
            try node.object_index.put(self.alloc, owned_name, old.len);
            node.children = entries;
            return &entries[old.len].node;
        }
        try node.object_index.put(self.alloc, owned_name, std.math.maxInt(usize));
        return null;
    }

    pub fn element(self: *View, node: *Node, index: usize) !?*Node {
        if (node.children) |entries| return if (index < entries.len) &entries[index].node else null;
        // A cached null is a requested index proven absent by validation.
        // Unplanned indices retain the ordinary lazy scan behavior.
        if (node.array_index.get(index)) |cached| return cached;
        var scanner = std.json.Scanner.initCompleteInput(self.alloc, node.raw);
        defer scanner.deinit();
        _ = try scanner.next();
        var i: usize = 0;
        while (try scanner.peekNextTokenType() != .array_end) : (i += 1) {
            switch (try scanner.peekNextTokenType()) {
                .object_end, .end_of_document => return error.SyntaxError,
                else => {},
            }
            const start = scanner.cursor;
            try scanner.skipValue();
            if (i == index) {
                const child = try self.alloc.create(Node);
                child.* = .{ .raw = node.raw[start..scanner.cursor] };
                try node.array_index.put(self.alloc, index, child);
                return child;
            }
        }
        return null;
    }

    pub const ProjectionPath = plan_mod.Path;
    pub fn projectionPaths(self: *View, fields: []const []const u8) ![]const ProjectionPath {
        var paths: std.ArrayListUnmanaged(ProjectionPath) = .empty;
        for (fields) |field_name| {
            if (field_name.len == 0 or field_name[0] == '-' or std.mem.eql(u8, field_name, "*")) continue;
            var parts: std.ArrayListUnmanaged([]const u8) = .empty;
            var tokens = std.mem.tokenizeScalar(u8, field_name, '.');
            while (tokens.next()) |part| try parts.append(self.alloc, part);
            if (parts.items.len != 0) try paths.append(self.alloc, .{ .parts = parts.items });
        }
        return paths.items;
    }
    pub fn projectionIndex(part: []const u8) ?usize {
        return plan_mod.index(part);
    }

    pub const DuplicatePolicy = enum { all, visited };
    pub const RootMode = enum { object_only, any_container };
    pub fn prepareProjection(view: *View, selector: *const Selector, policy: DuplicatePolicy, root_mode: RootMode) !void {
        return view.prepareProjectionWithOptions(selector, policy, root_mode, .{});
    }
    pub const PreparationOptions = struct {
        retain_root_fields: bool = false,
        // Streaming output retains requested numeric slots, but validates
        // fanout items without caching navigation for the whole array.
        defer_fanout: bool = false,
        root_excluded: []const []const u8 = &.{},
    };
    pub fn prepareProjectionWithOptions(view: *View, selector: *const Selector, policy: DuplicatePolicy, root_mode: RootMode, options: PreparationOptions) !void {
        const raw = view.root.raw;
        errdefer {
            view.root.raw = raw;
            view.invalidatePreparation();
        }
        var scanner = std.json.Scanner.initCompleteInput(@import("antfly_platform").allocator.reclaimingScratch(view.alloc), raw);
        defer scanner.deinit();
        try view.prepareProjectionValue(selector, policy, root_mode, options, &scanner, raw, @import("antfly_platform").allocator.reclaimingScratch(view.alloc));
        if (try scanner.next() != .end_of_document) return error.SyntaxError;
    }

    fn invalidatePreparation(view: *View) void {
        view.root.children = null;
        view.root.object_index = .empty;
        view.root.array_index.clearRetainingCapacity();
        view.root.state = .{};
    }

    /// Consume one value from a scanner over `raw`, leaving its enclosing
    /// container positioned at the next separator. Captured spans use the same
    /// immutable backing bytes; navigation belongs only to this view's arena.
    pub fn prepareProjectionValue(view: *View, selector: *const Selector, policy: DuplicatePolicy, root_mode: RootMode, options: PreparationOptions, scanner: *std.json.Scanner, raw: []const u8, work: Allocator) !void {
        errdefer {
            // A failed preparation cannot publish half-built caches to a
            // later predicate or projection using this same source view.
            view.invalidatePreparation();
        }
        const alloc = view.alloc;
        const root_start = scanner.cursor;
        const scratch = work;
        const Frame = struct {
            object: bool = false,
            key: bool = true,
            arena: std.heap.ArenaAllocator,
            names: std.StringHashMapUnmanaged(void) = .empty,
            node: ?*View.Node = null,
            children: std.ArrayListUnmanaged(View.Child) = .empty,
            selected: ?*const Selector = null,
            name: []const u8 = "",
            retain: bool = false,
            retain_all: bool = false,
            defer_items: bool = false,
            omitted: bool = false,
            excluded: []const []const u8 = &.{},
            ordinal: usize = 0,
            start: usize = 0,
            pending_node: ?*View.Node = null,
            // Stable across work-stack growth; reused for siblings. Only the
            // finished child is copied into the view's retained navigation.
            pending_storage: *View.Node,
            pending_selector: ?*const Selector = null,

            fn wants(self: *@This(), name: []const u8) bool {
                if (self.selected == null) return false;
                for (self.excluded) |omit| if (std.mem.eql(u8, omit, "*") or std.mem.eql(u8, omit, name)) return false;
                return self.retain_all or self.selected.?.fields.contains(name);
            }

            fn plan(self: *@This(), a: Allocator) !void {
                const node = self.node orelse return;
                if (self.object) {
                    node.children = null;
                    node.object_index.clearRetainingCapacity();
                    if (!self.retain_all and self.selected.?.fields.count() <= 8) node.object_index = try ObjectIndex.selected(a, self.selected.?);
                    node.markObjectComplete(false);
                    return;
                }
                node.state.fanout_keys_validated = false;
                const selected = self.selected orelse return;
                var indices = selected.indices.iterator();
                while (indices.next()) |entry| try self.node.?.array_index.put(a, entry.key_ptr.*, null);
            }
            fn beginValue(self: *@This(), start: usize, object_value: bool) !void {
                self.start = start;
                self.pending_node = null;
                self.pending_selector = null;
                const selected = self.selected orelse return;
                if (self.object) {
                    if (!self.retain) return;
                    if (selected.fields.get(self.name)) |child| {
                        if (child.fields.count() != 0) {
                            self.pending_selector = child;
                            if (self.node != null) self.pending_node = self.pending_storage;
                        }
                    }
                } else {
                    const item = selected.array_items.get(self.ordinal);
                    if (item == null and selected.fanout == null) return;
                    if (self.node != null and (!self.defer_items or item != null)) self.pending_node = self.pending_storage;
                    if (object_value) {
                        const child = item orelse selected.fanout.?;
                        if (child.fields.count() != 0) self.pending_selector = child;
                    }
                }
                if (self.pending_node) |node| node.* = .{ .raw = &.{} };
            }
            fn finishValue(self: *@This(), a: Allocator, source: []const u8, end: usize) !void {
                if (self.node) |parent| {
                    const span = source[self.start..end];
                    if (self.pending_node) |node| node.raw = span;
                    if (self.object) {
                        if (!self.retain) {
                            self.omitted = true;
                            self.ordinal += 1;
                            return;
                        }
                        try parent.object_index.put(a, self.name, self.children.items.len);
                        // Tiny selectors should not reserve eight full nodes
                        // per object. Allocate lazily and bound the first slab
                        // by the known selector width, never the source width.
                        if (self.children.capacity == 0 and !self.retain_all) try self.children.ensureTotalCapacityPrecise(a, @min(self.selected.?.fields.count(), 8));
                        try self.children.append(a, .{ .node = if (self.pending_node) |node| node.* else .{ .raw = span } });
                    } else if (self.pending_node) |node| {
                        if (self.selected.?.fanout != null and !self.defer_items) try self.children.append(a, .{ .node = node.* }) else {
                            const owned = try a.create(View.Node);
                            owned.* = node.*;
                            try parent.array_index.put(a, self.ordinal, owned);
                        }
                    }
                }
                self.ordinal += 1;
            }
            fn finishContainer(self: *@This(), a: Allocator) !void {
                if (self.node) |node| {
                    if (!self.object and self.selected.?.fanout != null) node.state.fanout_keys_validated = true;
                    if (self.object or (self.selected.?.fanout != null and !self.defer_items)) node.children = try self.children.toOwnedSlice(a);
                    if (self.object) {
                        node.markObjectComplete(!self.omitted);
                        if (!node.objectComplete()) {
                            var it = self.selected.?.fields.iterator();
                            while (it.next()) |entry| {
                                if (node.object_index.contains(entry.key_ptr.*)) continue;
                                // Missing-name cache keys belong to the view,
                                // which can outlive its borrowed selector plan.
                                const name = try a.dupe(u8, entry.key_ptr.*);
                                try node.object_index.put(a, name, std.math.maxInt(usize));
                            }
                        }
                    }
                }
            }
        };
        var frames: std.ArrayListUnmanaged(Frame) = .empty;
        defer {
            for (frames.items) |*frame| {
                frame.arena.deinit();
                scratch.destroy(frame.pending_storage);
            }
            frames.deinit(scratch);
        }
        var depth: usize = 0;
        var root_seen = false;
        while (!root_seen or depth != 0) {
            const token = try scanner.peekNextTokenType();
            if (depth != 0) {
                const frame = &frames.items[depth - 1];
                if ((frame.object and frame.key and token == .object_end) or (!frame.object and token == .array_end)) {
                    _ = try scanner.next();
                    try frame.finishContainer(alloc);
                    depth -= 1;
                    if (depth != 0) try frames.items[depth - 1].finishValue(alloc, raw, scanner.cursor);
                    continue;
                }
                if (frame.object and frame.key) {
                    if (policy == .visited and frame.selected == null) {
                        // Unvisited native keys need syntax checks, not decoded
                        // names or duplicate-key tables. Keep them allocation-free.
                        if (token != .string) return error.SyntaxError;
                        try scanner.skipValue();
                        frame.key = false;
                        continue;
                    }
                    const name = switch (try scanner.nextAllocMax(frame.arena.allocator(), .alloc_if_needed, raw.len)) {
                        .string, .allocated_string => |key_name| key_name,
                        else => return error.SyntaxError,
                    };
                    if (policy == .all or frame.selected != null) if ((try frame.names.getOrPut(frame.arena.allocator(), name)).found_existing) return error.DuplicateField;
                    frame.retain = frame.wants(name);
                    frame.name = if (frame.retain and frame.node != null) (frame.node.?.object_index.canonicalName(name) orelse try alloc.dupe(u8, name)) else name;
                    frame.key = false;
                    continue;
                }
                frame.key = true;
                try frame.beginValue(scanner.cursor, token == .object_begin);
            }
            root_seen = true;
            if (token == .object_begin or token == .array_begin) {
                const selected_node: ?*View.Node = if (depth == 0) (if (token == .object_begin or root_mode == .any_container) &view.root else null) else if (frames.items[depth - 1].pending_selector != null) frames.items[depth - 1].pending_node else null;
                const selected_plan = if (depth == 0) (if (token == .object_begin or root_mode == .any_container) selector else null) else frames.items[depth - 1].pending_selector;
                _ = try scanner.next();
                if (depth == frames.items.len) {
                    try frames.ensureUnusedCapacity(scratch, 1);
                    frames.appendAssumeCapacity(.{ .arena = .init(scratch), .pending_storage = try scratch.create(View.Node) });
                }
                const frame = &frames.items[depth];
                var key_arena = frame.arena;
                _ = key_arena.reset(.{ .retain_with_limit = 16 * 1024 });
                frame.* = .{ .pending_storage = frame.pending_storage, .arena = key_arena, .object = token == .object_begin, .node = selected_node, .selected = selected_plan, .defer_items = options.defer_fanout and token == .array_begin and selected_plan != null and selected_plan.?.fanout != null, .retain_all = depth == 0 and options.retain_root_fields, .excluded = if (depth == 0) options.root_excluded else &.{} };
                try frame.plan(alloc);
                depth += 1;
            } else {
                if (token == .object_end or token == .array_end or token == .end_of_document) return error.SyntaxError;
                try scanner.skipValue();
                if (depth != 0) try frames.items[depth - 1].finishValue(alloc, raw, scanner.cursor);
            }
        }
        view.root.raw = raw[root_start..scanner.cursor];
    }

    pub fn materialize(self: *View, node: *Node) !Value {
        switch (node.logical) {
            .empty => {},
            .container => |value| return value.*,
            .null => return .null,
            .bool => |value| return .{ .bool = value },
            .integer => |value| return .{ .integer = value },
            .float => |value| return .{ .float = value },
            .string => |value| return .{ .string = value.* },
            .number_string => |value| return .{ .number_string = value.* },
        }
        const value = try std.json.parseFromSliceLeaky(Value, self.alloc, node.raw, .{});
        node.logical = switch (value) {
            .null => .null,
            .bool => |v| .{ .bool = v },
            .integer => |v| .{ .integer = v },
            .float => |v| .{ .float = v },
            .string, .number_string => |v| blk: {
                const cached = try self.alloc.create([]const u8);
                cached.* = v;
                break :blk if (value == .string) .{ .string = cached } else .{ .number_string = cached };
            },
            .object, .array => blk: {
                const cached = try self.alloc.create(Value);
                cached.* = value;
                break :blk .{ .container = cached };
            },
        };
        self.materialized_values += 1;
        return value;
    }

    fn pointerIndex(segment: []const u8) bool {
        if (segment.len == 0 or (segment.len > 1 and segment[0] == '0')) return false;
        for (segment) |c| if (c < '0' or c > '9') return false;
        return true;
    }

    /// Dotted paths fan out through arrays; JSON pointers require a canonical
    /// numeric index. Iterative traversal also handles deeply nested arrays
    /// without turning user data depth into native call-stack depth.
    pub fn collect(self: *View, path: []const []const u8, pointer: bool, out: *std.ArrayListUnmanaged(Value)) !void {
        const Work = struct { node: *Node, depth: usize };
        var pending = std.ArrayListUnmanaged(Work).empty;
        defer pending.deinit(self.alloc);
        try pending.append(self.alloc, .{ .node = &self.root, .depth = 0 });
        while (pending.pop()) |work| {
            if (work.depth == path.len) {
                try out.append(self.alloc, try self.materialize(work.node));
                continue;
            }
            const part = path[work.depth];
            switch (work.node.kind()) {
                '{' => if (try self.field(work.node, part)) |node| {
                    try pending.append(self.alloc, .{ .node = node, .depth = work.depth + 1 });
                },
                '[' => {
                    if (pointer and !pointerIndex(part)) continue;
                    if (std.fmt.parseInt(usize, part, 10)) |i| {
                        if (try self.element(work.node, i)) |child| try pending.append(self.alloc, .{ .node = child, .depth = work.depth + 1 });
                    } else |_| if (!pointer) {
                        const entries = try self.children(work.node);
                        var i = entries.len;
                        while (i != 0) {
                            i -= 1;
                            try pending.append(self.alloc, .{ .node = &entries[i].node, .depth = work.depth });
                        }
                    }
                },
                else => {},
            }
        }
    }

    /// Apply one positive projection in caller order. Objects merge, numeric
    /// array selections compact to one item, array fanout visits object items,
    /// and later array selections replace earlier ones, just like lookupJson.
    /// Output belongs to a separate row arena; cached navigation can outlive it.
    pub fn include(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, name: []const u8, path: []const []const u8) !void {
        try self.includeNode(alloc, dst, name, &self.root, path);
    }

    /// Ordinary document projections share the native row path/array rules.
    pub fn includeRoot(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, path: []const []const u8) !void {
        if (path.len == 0) return;
        for (self.root_sources) |source| if (std.mem.eql(u8, source.name, path[0])) {
            return source.include(alloc, dst, path);
        };
        if (self.root.kind() != '{') return;
        if (try self.field(&self.root, path[0])) |node|
            try self.includeNode(alloc, dst, path[0], node, path[1..]);
    }

    pub fn includeRootExcept(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, excluded: []const []const u8) !void {
        return self.includeRootExceptImpl(alloc, dst, excluded, false);
    }
    /// Use only after prepareProjectionWithOptions retained every nonexcluded
    /// root member with these exclusions. Other callers use includeRootExcept.
    pub fn includePreparedRootExcept(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, excluded: []const []const u8) !void {
        return self.includeRootExceptImpl(alloc, dst, excluded, true);
    }
    fn includeRootExceptImpl(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, excluded: []const []const u8, prepared: bool) !void {
        for (excluded) |name| if (std.mem.eql(u8, name, "*")) return;
        const children_in_order: []Child = if (self.root.kind() == '{') (if (prepared) self.root.children orelse try self.children(&self.root) else try self.children(&self.root)) else &.{};
        const names = try self.alloc.alloc([]const u8, children_in_order.len);
        // Preserve source order without adding a name to every array node.
        var entries = self.root.object_index.iterator();
        while (entries.next()) |entry| if (entry.value_ptr.* < names.len) {
            names[entry.value_ptr.*] = entry.key_ptr.*;
        };
        for (children_in_order, names) |*child, field_name| {
            const omit = for (excluded) |name| {
                if (std.mem.eql(u8, name, field_name)) break true;
            } else false;
            if (!omit) {
                const replacement = for (self.root_sources) |source| {
                    if (std.mem.eql(u8, source.name, field_name)) break source;
                } else null;
                if (replacement) |source| try source.include(alloc, dst, &.{field_name}) else try self.includeNode(alloc, dst, field_name, &child.node, &.{});
            }
        }
        for (self.root_sources) |source| {
            // Planned misses occupy compact slots too. Only a real child
            // means this source was already emitted in primary field order.
            if (self.root.object_index.get(source.name)) |i| if (i < children_in_order.len) continue;
            const omit = for (excluded) |name| {
                if (std.mem.eql(u8, name, source.name)) break true;
            } else false;
            if (!omit) try source.include(alloc, dst, &.{source.name});
        }
    }

    fn includeNode(self: *View, alloc: Allocator, dst: *std.json.ObjectMap, name: []const u8, node: *Node, path: []const []const u8) anyerror!void {
        // A single selected field is common in fanout. Reserve one dense
        // entry instead of ArrayHashMap's default multi-entry growth slab.
        // Subsequent fields use the ordinary map growth/indexing contract.
        if (dst.entries.capacity == 0) try dst.entries.setCapacity(alloc, 1);
        if (path.len == 0) {
            // Projection mutates merged objects. Never lend a cached logical
            // container to it: subsequent paths must still see the original.
            const value = try std.json.parseFromSliceLeaky(Value, alloc, node.raw, .{ .parse_numbers = self.parse_numbers });
            try dst.put(alloc, name, value);
            return;
        }
        // Synthetic-field wildcards need a DOM only of that selected source,
        // never of the surrounding primary document. This also preserves the
        // legacy object/array wildcard rules for stored special fields.
        if (path.len == 1 and std.mem.eql(u8, path[0], "*")) {
            const value = try std.json.parseFromSliceLeaky(Value, alloc, node.raw, .{ .parse_numbers = self.parse_numbers });
            var source = std.json.ObjectMap.empty;
            try source.put(alloc, name, value);
            return @import("../document_query.zig").applyIncludePath(alloc, source, dst, &.{ name, "*" });
        }
        switch (node.kind()) {
            '{' => {
                const entry = try dst.getOrPut(alloc, name);
                if (!entry.found_existing or entry.value_ptr.* != .object) entry.value_ptr.* = .{ .object = .empty };
                if (try self.field(node, path[0])) |child| try self.includeNode(alloc, &entry.value_ptr.object, path[0], child, path[1..]);
            },
            '[' => {
                var result = std.json.Array.init(alloc);
                const index = projectionIndex(path[0]);
                if (index) |i| {
                    const item = (try self.element(node, i)) orelse return;
                    if (path.len == 1) {
                        try result.append(try std.json.parseFromSliceLeaky(Value, alloc, item.raw, .{ .parse_numbers = self.parse_numbers }));
                    } else if (item.kind() == '{') {
                        var object = std.json.ObjectMap.empty;
                        if (try self.field(item, path[1])) |child| try self.includeNode(alloc, &object, path[1], child, path[2..]);
                        if (object.count() != 0) try result.append(.{ .object = object });
                    }
                } else for (try self.children(node)) |*entry| {
                    if (entry.node.kind() != '{') continue;
                    var object = std.json.ObjectMap.empty;
                    if (try self.field(&entry.node, path[0])) |child| try self.includeNode(alloc, &object, path[0], child, path[1..]);
                    if (object.count() != 0) try result.append(.{ .object = object });
                }
                if (result.items.len != 0) try dst.put(alloc, name, .{ .array = result });
            },
            else => {},
        }
    }
};

test "relational lazy JSON paths skip unrelated DOM and preserve pointer semantics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    var view = View.init(alloc, "{\"a\\u002fb\":[{\"n\":9007199254740993},{\"n\":null}],\"skip\":[[1,2,3]]}");
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{ "a/b", "n" }, false, &values);
    try std.testing.expectEqual(@as(usize, 2), values.items.len);
    try std.testing.expectEqual(@as(i64, 9007199254740993), values.items[0].integer);
    try std.testing.expect(values.items[1] == .null);
    try std.testing.expect((try view.field(&view.root, "skip")).?.children == null);
    values.clearRetainingCapacity();
    try view.collect(&.{ "a/b", "01", "n" }, true, &values);
    try std.testing.expectEqual(@as(usize, 0), values.items.len);
    try view.collect(&.{ "a/b", "0", "n" }, true, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
}

test "relational lazy JSON leaf and numeric array projection have bounded allocation" {
    const alloc = std.testing.allocator;
    const items = try alloc.alloc(u8, 256 * 1024);
    defer alloc.free(items);
    for (items, 0..) |*byte, i| byte.* = if (i % 2 == 0) '0' else ',';
    const raw = try std.fmt.allocPrint(alloc, "{{\"match\":1,\"items\":[{s}7]}}", .{items});
    defer alloc.free(raw);
    var buffer: [16 * 1024]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    var view = View.init(fixed.allocator(), raw);
    var result = std.json.ObjectMap.empty;
    try view.include(fixed.allocator(), &result, "payload", &.{"match"});
    const leaf = try std.json.Stringify.valueAlloc(fixed.allocator(), Value{ .object = result }, .{});
    try std.testing.expectEqualStrings("{\"payload\":{\"match\":1}}", leaf);
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{ "items", "131072" }, true, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
    try std.testing.expectEqual(@as(i64, 7), values.items[0].integer);
    try std.testing.expect((try view.field(&view.root, "items")).?.children == null);
    result = .empty;
    try view.include(fixed.allocator(), &result, "payload", &.{ "items", "131072" });
    const indexed = try std.json.Stringify.valueAlloc(fixed.allocator(), Value{ .object = result }, .{});
    try std.testing.expectEqualStrings("{\"payload\":{\"items\":[7]}}", indexed);
}

test "relational lazy JSON errors release all arenas and deep fanout is iterative" {
    const alloc = std.testing.allocator;
    const Check = struct {
        fn run(failing: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(failing);
            defer arena.deinit();
            const scratch = arena.allocator();
            var view = View.init(scratch, "{\"a\\u002fb\":[{\"n\":\"escaped\\nvalue\"},{\"n\":null}],\"x\":1}");
            var values = std.ArrayListUnmanaged(Value).empty;
            try view.collect(&.{ "a/b", "n" }, false, &values);
            var output = std.json.ObjectMap.empty;
            try view.include(scratch, &output, "payload", &.{ "a/b", "n" });
            try view.include(scratch, &output, "payload", &.{ "a/b", "0" });
            try view.include(scratch, &output, "payload", &.{});
            try view.include(scratch, &output, "payload", &.{"x"});
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{});
    for ([_][]const u8{ "{\"x\":[1,]}", "{\"x\":1,\"x\":2}" }) |raw| {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var view = View.init(arena.allocator(), raw);
        var values = std.ArrayListUnmanaged(Value).empty;
        if (view.collect(&.{"x"}, false, &values)) |_| return error.TestExpectedError else |_| {}
    }
    {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        var view = View.init(arena.allocator(), "[1,]");
        var values: std.ArrayListUnmanaged(Value) = .empty;
        var index_buffer: [64]u8 = undefined;
        const index = try std.fmt.bufPrint(&index_buffer, "{d}", .{std.math.maxInt(usize)});
        try std.testing.expectError(error.SyntaxError, view.collect(&.{index}, true, &values));
    }
    const depth = 1024;
    const raw = try alloc.alloc(u8, 2 * depth + 7);
    defer alloc.free(raw);
    @memset(raw[0..depth], '[');
    @memcpy(raw[depth..][0..7], "{\"n\":1}");
    @memset(raw[depth + 7 ..], ']');
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var view = View.init(arena.allocator(), raw);
    var values = std.ArrayListUnmanaged(Value).empty;
    try view.collect(&.{"n"}, false, &values);
    try std.testing.expectEqual(@as(usize, 1), values.items.len);
}

test "public graph metric borrowed wire partial JSON object caches survive predicate and plan reuse" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    var view = View.init(s, "{\"skip\":2,\"keep\":1,\"other\":{\"a\":3}}");
    var values: std.ArrayListUnmanaged(Value) = .empty;
    try view.collect(&.{"skip"}, false, &values);
    try std.testing.expectEqual(@as(i64, 2), values.items[0].integer);
    var plan_arena = std.heap.ArenaAllocator.init(a);
    var live = true;
    defer if (live) plan_arena.deinit();
    const selector = try plan_mod.compile(plan_arena.allocator(), &.{ .{ .parts = &.{"keep"} }, .{ .parts = &.{"missing"} } });
    try view.prepareProjection(selector, .visited, .object_only);
    try std.testing.expectEqual(@as(usize, 1), view.root.children.?.len);
    plan_arena.deinit();
    live = false;
    values.clearRetainingCapacity();
    try view.collect(&.{"missing"}, false, &values);
    try std.testing.expectEqual(@as(usize, 0), values.items.len);
    try std.testing.expectEqual(@as(usize, 1), view.root.children.?.len);
    try view.collect(&.{ "other", "a" }, false, &values);
    try std.testing.expectEqual(@as(i64, 3), values.items[0].integer);
    values.clearRetainingCapacity();
    try view.collect(&.{"skip"}, false, &values);
    try std.testing.expectEqual(@as(i64, 2), values.items[0].integer);
    const keep = try plan_mod.compile(s, &.{.{ .parts = &.{"keep"} }});
    try view.prepareProjection(keep, .visited, .object_only);
    var all: std.json.ObjectMap = .empty;
    try view.includeRootExcept(s, &all, &.{});
    const body = try std.json.Stringify.valueAlloc(s, Value{ .object = all }, .{});
    try std.testing.expectEqualStrings(view.root.raw, body);
}

test "public graph metric borrowed wire compact object projections retain unplanned fields across allocation faults" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const s = arena.allocator();
            const raw = "{\"obj\":{\"k0\":0,\"k1\":1,\"k2\":2,\"k3\":3,\"k4\":4,\"k5\":5,\"k6\":6,\"k7\":7,\"k8\":8,\"k9\":9}}";
            const fields = [_][]const u8{ "obj.k0", "obj.k1", "obj.k2", "obj.k3", "obj.k4", "obj.k5", "obj.k6", "obj.k7", "obj.k8" };
            for ([_]usize{ 1, 8, 9 }) |count| {
                var view = View.init(s, raw);
                view.parse_numbers = true;
                {
                    var plan = (try @import("../document_query.zig").SpanProjectionPlan.init(a, .{ .fields = fields[0..count], .include_all_fields = false })).?;
                    defer plan.deinit();
                    try view.prepareProjection(plan.selector, .all, .object_only);
                    var result: std.json.ObjectMap = .empty;
                    for (plan.paths) |path| try view.includeRoot(s, &result, path.parts);
                    const obj = result.get("obj").?.object;
                    try std.testing.expectEqual(count, obj.count());
                    for (0..count) |i| {
                        var name: [16]u8 = undefined;
                        const key = try std.fmt.bufPrint(&name, "k{d}", .{i});
                        try std.testing.expectEqual(@as(i64, @intCast(i)), obj.get(key).?.integer);
                    }
                }
                // Rebuild a complete lazy index after the selector owner dies.
                var values: std.ArrayListUnmanaged(Value) = .empty;
                try view.collect(&.{ "obj", "k9" }, false, &values);
                try std.testing.expectEqual(@as(i64, 9), values.items[0].integer);
                values.clearRetainingCapacity();
                try view.collect(&.{ "obj", "k0" }, false, &values);
                try std.testing.expectEqual(@as(i64, 0), values.items[0].integer);
            }
            var empty = View.init(s, "{}");
            {
                var plan = (try @import("../document_query.zig").SpanProjectionPlan.init(a, .{ .fields = &.{"missing"}, .include_all_fields = false })).?;
                defer plan.deinit();
                try empty.prepareProjection(plan.selector, .all, .object_only);
            }
            empty.root_sources = &.{.{ .name = "missing", .value = .{ .integer = 5 } }};
            var result: std.json.ObjectMap = .empty;
            try empty.includeRootExcept(s, &result, &.{});
            try std.testing.expectEqual(@as(i64, 5), result.get("missing").?.integer);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire streaming preparation recovers after every allocation failure" {
    const a = std.testing.allocator;
    const selector_paths = [_]plan_mod.Path{ .{ .parts = &.{"x"} }, .{ .parts = &.{"missing"} } };
    var plan_arena = std.heap.ArenaAllocator.init(a);
    defer plan_arena.deinit();
    const selector = try plan_mod.compile(plan_arena.allocator(), &selector_paths);
    var failed: usize = 0;
    var index: usize = 0;
    while (true) : (index += 1) {
        var induced = false;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var failing = std.testing.FailingAllocator.init(arena.allocator(), .{ .fail_index = index });
        var view = View.init(failing.allocator(), "{\"x\":1,\"y\":2}");
        view.prepareProjection(selector, .all, .object_only) catch |err| {
            if (err != error.OutOfMemory) return err;
            failed += 1;
            induced = true;
            failing.fail_index = std.math.maxInt(usize);
            var values: std.ArrayListUnmanaged(Value) = .empty;
            try view.collect(&.{"x"}, false, &values);
            try std.testing.expectEqual(@as(usize, 1), values.items.len);
            try std.testing.expectEqual(@as(i64, 1), values.items[0].integer);
            values.clearRetainingCapacity();
            try view.collect(&.{"missing"}, false, &values);
            try std.testing.expectEqual(@as(usize, 0), values.items.len);
            try view.prepareProjection(selector, .all, .object_only);
            try view.collect(&.{"x"}, false, &values);
            try std.testing.expectEqual(@as(i64, 1), values.items[0].integer);
        };
        if (!induced) break;
    }
    try std.testing.expect(failed != 0);
}

test "public graph metric borrowed wire streaming deferred fanout preserves object only root validation policy" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const selector = try plan_mod.compile(arena.allocator(), &.{.{ .parts = &.{"x"} }});
    var ignored = View.init(arena.allocator(), "[{\"x\":1,\"x\":2}]");
    try ignored.prepareProjectionWithOptions(selector, .visited, .object_only, .{ .defer_fanout = true });
    try std.testing.expect(ignored.root.children == null);
    var strict = View.init(arena.allocator(), ignored.root.raw);
    try std.testing.expectError(error.DuplicateField, strict.prepareProjectionWithOptions(selector, .all, .object_only, .{ .defer_fanout = true }));
}
