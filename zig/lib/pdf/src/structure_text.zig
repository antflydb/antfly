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

//! Page-local, lazy ParentTree lookup. Container keys scope MCIDs; element
//! ancestry and decoded replacements are memoized across page/Form content.
//! The caller supplies a bounded allocator, including for object resolution.
const std = @import("std");
const syntax = @import("syntax.zig");
const encoding = @import("text_encoding.zig");
const A = std.mem.Allocator;
const Object = syntax.Object;

pub const Resolver = struct {
    context: *const anyopaque,
    root: *const fn (*const anyopaque, A) anyerror!Object,
    resolve: *const fn (*const anyopaque, A, *const Object) anyerror!Object,
    check: *const fn (*const anyopaque) anyerror!void,
    exhausted: *const fn (*const anyopaque) bool,
};

pub const Replacement = struct { owner: u64, text: []const u8 };

fn refKey(ref: syntax.ObjRef) u64 {
    return (@as(u64, ref.id) << 16) | ref.gen;
}

fn reference(value: *const Object) !syntax.ObjRef {
    return if (value.* == .obj_ref) value.obj_ref else error.InvalidPdfStructure;
}

const Element = struct {
    object: Object,
    state: enum { pending, visiting, done, invalid } = .pending,
    owner: ?u64 = null,
    page: ?syntax.ObjRef = null,
    text: ?[]u8 = null,
    bindings: std.AutoHashMapUnmanaged(Binding, void) = .empty,
};

const Binding = struct { container: i64, mcid: i64 };

/// Reader-local metadata cache. Callers bound its allocator and give render
/// forks independent instances; page lookups only borrow immutable objects.
pub const Document = struct {
    alloc: A,
    objects: std.AutoHashMapUnmanaged(u64, Object) = .empty,
    validated_nums: std.AutoHashMapUnmanaged(usize, void) = .empty,
    root_value: Object = .null,
    checked_root: bool = false,

    pub fn deinit(self: *Document) void {
        var objects = self.objects.valueIterator();
        while (objects.next()) |entry| entry.deinit(self.alloc);
        self.objects.deinit(self.alloc);
        self.validated_nums.deinit(self.alloc);
        self.root_value.deinit(self.alloc);
    }

    fn root(self: *Document, resolver: Resolver) !Object {
        if (!self.checked_root) {
            self.root_value = try resolver.root(resolver.context, self.alloc);
            self.checked_root = true;
        }
        return self.root_value;
    }

    fn object(self: *Document, resolver: Resolver, value: *const Object) !Object {
        if (value.* != .obj_ref) return value.*;
        const id = refKey(value.obj_ref);
        if (self.objects.get(id)) |cached| return cached;
        var resolved = try resolver.resolve(resolver.context, self.alloc, value);
        errdefer resolved.deinit(self.alloc);
        try self.objects.put(self.alloc, id, resolved);
        return resolved;
    }
};

pub const Lookup = struct {
    alloc: A,
    resolver: Resolver,
    page: ?syntax.ObjRef,
    page_parent: ?i64 = null,
    document: ?*Document = null,
    max_nodes: usize = 100_000,
    work: usize = 0,
    checked_root: bool = false,
    root_value: Object = .null,
    parent_tree: Object = .null,
    number_nodes: std.AutoHashMapUnmanaged(u64, Object) = .empty,
    validated_nums: std.AutoHashMapUnmanaged(usize, void) = .empty,
    // Borrow arrays from number_nodes/parent_tree instead of cloning a whole
    // flat ParentTree (or its MCID arrays) for each Form/container lookup.
    containers: std.AutoHashMapUnmanaged(i64, Object) = .empty,
    elements: std.AutoHashMapUnmanaged(u64, Element) = .empty,
    stream_containers: std.AutoHashMapUnmanaged(u64, i64) = .empty,
    // Only stable named-property or cached-object strings use this index.
    // Inline operand strings are short lived and must never be pointer keys.
    property_text: std.AutoHashMapUnmanaged(usize, union(enum) { text: []u8, invalid }) = .empty,
    incomplete: bool = false,

    pub fn deinit(self: *Lookup) void {
        self.containers.deinit(self.alloc);
        self.validated_nums.deinit(self.alloc);
        var numbers = self.number_nodes.valueIterator();
        while (numbers.next()) |object| object.deinit(self.alloc);
        self.number_nodes.deinit(self.alloc);
        self.root_value.deinit(self.alloc);
        self.stream_containers.deinit(self.alloc);
        var texts = self.property_text.valueIterator();
        while (texts.next()) |value| if (value.* == .text) self.alloc.free(value.text);
        self.property_text.deinit(self.alloc);
        var elements = self.elements.valueIterator();
        while (elements.next()) |element| {
            if (element.text) |text| self.alloc.free(text);
            element.bindings.deinit(self.alloc);
        }
        self.elements.deinit(self.alloc);
    }

    fn step(self: *Lookup, depth: usize) !void {
        try self.resolver.check(self.resolver.context);
        if (depth > 128 or self.work >= self.max_nodes) return error.PdfStructureLimitExceeded;
        self.work += 1;
    }

    fn resolve(self: *Lookup, value: *const Object) !Object {
        try self.step(0);
        return self.resolver.resolve(self.resolver.context, self.alloc, value);
    }

    fn root(self: *Lookup) !void {
        if (self.checked_root) return;
        self.checked_root = true;
        const raw = if (self.document) |document| try document.root(self.resolver) else blk: {
            self.root_value = try self.resolver.root(self.resolver.context, self.alloc);
            break :blk self.root_value;
        };
        if (raw == .null) return;
        const tree = try self.numberObject(&raw);
        const parent = tree.get("ParentTree") orelse return error.InvalidPdfStructure;
        self.parent_tree = parent.*;
    }

    fn numberObject(self: *Lookup, value: *const Object) !Object {
        if (value.* != .obj_ref) return value.*;
        if (self.document) |document| {
            if (!document.objects.contains(refKey(value.obj_ref))) try self.step(0);
            return document.object(self.resolver, value);
        }
        const id = refKey(value.obj_ref);
        if (self.number_nodes.get(id)) |cached| return cached;
        var object = try self.resolve(value);
        errdefer object.deinit(self.alloc);
        try self.number_nodes.put(self.alloc, id, object);
        return object;
    }

    fn integerValue(self: *Lookup, raw: *const Object) !i64 {
        const value = try self.numberObject(raw);
        return value.asInteger() orelse error.InvalidPdfStructure;
    }

    fn optionalValue(self: *Lookup, raw: *const Object) !?Object {
        const value = try self.numberObject(raw);
        return if (value == .null) null else value;
    }

    fn pageReference(self: *Lookup, raw: ?*const Object, inherited: ?syntax.ObjRef) !?syntax.ObjRef {
        const value = raw orelse return inherited;
        if (value.* == .null) return inherited;
        const ref = try reference(value);
        // The page index already validates this object. Preserve identity
        // without reparsing/cloning a page and its resource dictionaries.
        if (self.page) |current| if (refKey(current) == refKey(ref)) return ref;
        if (try self.optionalValue(value) == null) return inherited;
        return ref;
    }

    fn propertyTextImpl(self: *Lookup, raw: *const Object) !?[]const u8 {
        const value = (try self.optionalValue(raw)) orelse return null;
        if (value != .string) return error.InvalidPdfStructure;
        const key = if (value.string.len == 0) 0 else @intFromPtr(value.string.ptr);
        if (self.property_text.get(key)) |cached| return switch (cached) {
            .text => |text| text,
            .invalid => error.InvalidPdfStructure,
        };
        try self.step(0);
        const text = encoding.pdfDocDecodeAlloc(self.alloc, value.string) catch |err| {
            if (err == error.InvalidUtf16) try self.property_text.put(self.alloc, key, .invalid);
            return err;
        };
        errdefer self.alloc.free(text);
        try self.property_text.put(self.alloc, key, .{ .text = text });
        return text;
    }

    pub fn propertyText(self: *Lookup, raw: *const Object) !?[]const u8 {
        try self.resolver.check(self.resolver.context);
        return self.propertyTextImpl(raw) catch |err| {
            try self.noteFailure(err);
            return null;
        };
    }

    // Non-root number-tree nodes carry Limits. Binary search avoids walking
    // unrelated page entries; selected paths are depth/work bounded even when
    // a malformed number tree links back to an ancestor.
    fn numberValue(self: *Lookup, raw: *const Object, key: i64, depth: usize) anyerror!?Object {
        try self.step(depth);
        const node = try self.numberObject(raw);
        if (node != .dict) return error.InvalidPdfStructure;
        if (node.get("Nums")) |nums_raw| {
            const nums = try self.numberObject(nums_raw);
            if (nums != .array or nums.array.len % 2 != 0) return error.InvalidPdfStructure;
            const identity = @intFromPtr(nums.array.ptr);
            const validated = if (self.document) |document| &document.validated_nums else &self.validated_nums;
            const validated_alloc = if (self.document) |document| document.alloc else self.alloc;
            if (!validated.contains(identity)) {
                if (nums.array.len / 2 > self.max_nodes) return error.PdfStructureLimitExceeded;
                var previous: i64 = -1;
                var index: usize = 0;
                while (index < nums.array.len) : (index += 2) {
                    if (index % 256 == 0) try self.step(depth);
                    const number = try self.integerValue(&nums.array[index]);
                    if (number <= previous) return error.InvalidPdfStructure;
                    previous = number;
                }
                try validated.put(validated_alloc, identity, {});
            }
            var low: usize = 0;
            var high = nums.array.len / 2;
            while (low < high) {
                const mid = low + (high - low) / 2;
                const number = try self.integerValue(&nums.array[mid * 2]);
                if (number < key) low = mid + 1 else if (number > key) high = mid else return nums.array[mid * 2 + 1];
            }
            return null;
        }
        const kids_raw = node.get("Kids") orelse return error.InvalidPdfStructure;
        const kids = try self.numberObject(kids_raw);
        if (kids != .array) return error.InvalidPdfStructure;
        var low: usize = 0;
        var high = kids.array.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const child = try self.numberObject(&kids.array[mid]);
            const limits = try self.numberObject(child.get("Limits") orelse return error.InvalidPdfStructure);
            if (limits != .array or limits.array.len != 2) return error.InvalidPdfStructure;
            const first = try self.integerValue(&limits.array[0]);
            const last = try self.integerValue(&limits.array[1]);
            if (first > last) return error.InvalidPdfStructure;
            if (key < first) high = mid else if (key > last) low = mid + 1 else return self.numberValue(&kids.array[mid], key, depth + 1);
        }
        return null;
    }

    fn loadElement(self: *Lookup, ref: syntax.ObjRef, nullable: bool) !bool {
        const id = refKey(ref);
        if (self.elements.contains(id)) return true;
        const raw = Object{ .obj_ref = ref };
        const object = try self.numberObject(&raw);
        // An unassigned ParentTree slot can be direct or indirect null. A
        // structure element's P chain, however, must reach a real element/root.
        if (nullable and object == .null) return false;
        if (object != .dict) return error.InvalidPdfStructure;
        try self.elements.put(self.alloc, id, .{ .object = object });
        return true;
    }

    fn pageMatches(self: *Lookup, page: ?syntax.ObjRef) !void {
        const current = self.page orelse return error.InvalidPdfStructure;
        const target = page orelse return error.InvalidPdfStructure;
        // Cross-page replacements cannot be assigned faithfully to independent
        // page units. Retain native glyphs and explicitly report partial coverage.
        if (refKey(current) != refKey(target)) return error.UnsupportedPdfStructureText;
    }

    fn streamContainer(self: *Lookup, raw: *const Object) !?i64 {
        if (raw.* == .null) return null;
        const id = refKey(try reference(raw));
        if (self.stream_containers.get(id)) |key| return key;
        const stream = try self.numberObject(raw);
        if (stream == .null) return null;
        if (stream != .stream) return error.InvalidPdfStructure;
        const parent = stream.get("StructParents") orelse return error.UnsupportedPdfStructureText;
        const key = try self.integerValue(parent);
        if (key < 0) return error.InvalidPdfStructure;
        try self.stream_containers.put(self.alloc, id, key);
        return key;
    }

    fn validateContent(self: *Lookup, value: *const Object, inherited_page: ?syntax.ObjRef, depth: usize, visited: *std.AutoHashMapUnmanaged(u64, void), bindings: *std.AutoHashMapUnmanaged(Binding, void)) anyerror!void {
        try self.step(depth);
        if (value.* == .integer) {
            if (value.integer < 0) return error.InvalidPdfStructure;
            try self.pageMatches(inherited_page);
            return bindings.put(self.alloc, .{ .container = self.page_parent orelse return error.UnsupportedPdfStructureText, .mcid = value.integer }, {});
        }
        if (value.* == .array) {
            for (value.array) |*child| try self.validateContent(child, inherited_page, depth + 1, visited, bindings);
            return;
        }
        if (value.* == .obj_ref) {
            const id = refKey(value.obj_ref);
            const entry = try visited.getOrPut(self.alloc, id);
            if (entry.found_existing) return error.InvalidPdfStructure;
            const resolved = try self.numberObject(value);
            return self.validateContent(&resolved, inherited_page, depth + 1, visited, bindings);
        }
        if (value.* != .dict) return error.UnsupportedPdfStructureText;
        const page = try self.pageReference(value.get("Pg"), inherited_page);
        if (value.get("MCID")) |mcid| {
            const index = try self.integerValue(mcid);
            if (index < 0) return error.InvalidPdfStructure;
            try self.pageMatches(page);
            const container = (if (value.get("Stm")) |stream| try self.streamContainer(stream) else null) orelse self.page_parent orelse return error.UnsupportedPdfStructureText;
            return bindings.put(self.alloc, .{ .container = container, .mcid = index }, {});
        }
        if (value.get("S") == null) return error.UnsupportedPdfStructureText; // e.g. OBJR
        const kids = value.get("K") orelse return error.UnsupportedPdfStructureText;
        try self.validateContent(kids, page, depth + 1, visited, bindings);
    }

    fn owner(self: *Lookup, ref: syntax.ObjRef, depth: usize, nullable: bool) anyerror!?u64 {
        if (!try self.loadElement(ref, nullable)) return null;
        const id = refKey(ref);
        const saved = self.elements.get(id).?;
        switch (saved.state) {
            .done => return saved.owner,
            .visiting, .invalid => return error.InvalidPdfStructure,
            .pending => {},
        }
        try self.step(depth);
        self.elements.getPtr(id).?.state = .visiting;
        errdefer self.elements.getPtr(id).?.state = .invalid;
        const object = saved.object; // dictionary storage survives hash-map growth
        if (object.get("Type")) |raw| {
            const kind = try self.numberObject(raw);
            if (std.mem.eql(u8, kind.asName() orelse "", "StructTreeRoot")) {
                self.elements.getPtr(id).?.state = .done;
                return null;
            }
        }
        if (object.get("S") == null) return error.InvalidPdfStructure;
        const parent = try reference(object.get("P") orelse return error.InvalidPdfStructure);
        const ancestor = try self.owner(parent, depth + 1, false);
        const inherited_page = self.elements.get(refKey(parent)).?.page;
        const page = try self.pageReference(object.get("Pg"), inherited_page);
        self.elements.getPtr(id).?.page = page;
        var selected = ancestor;
        if (selected == null) if (object.get("ActualText")) |raw| if (try self.optionalValue(raw)) |actual| {
            if (actual != .string) return error.InvalidPdfStructure;
            var visited: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer visited.deinit(self.alloc);
            var bindings: std.AutoHashMapUnmanaged(Binding, void) = .empty;
            errdefer bindings.deinit(self.alloc);
            try visited.put(self.alloc, id, {});
            try self.validateContent(object.get("K") orelse return error.UnsupportedPdfStructureText, page, 0, &visited, &bindings);
            const text = try encoding.pdfDocDecodeAlloc(self.alloc, actual.string);
            self.elements.getPtr(id).?.text = text;
            self.elements.getPtr(id).?.bindings = bindings;
            selected = id;
        };
        const result = self.elements.getPtr(id).?;
        result.owner = selected;
        result.state = .done;
        return selected;
    }

    fn getImpl(self: *Lookup, container: ?i64, mcid: i64) !?Replacement {
        try self.root();
        if (self.parent_tree == .null) {
            if (container != null) self.incomplete = true;
            return null;
        }
        const key = container orelse return error.InvalidPdfStructure;
        if (key < 0 or mcid < 0) return error.InvalidPdfStructure;
        if (!self.containers.contains(key)) {
            const raw = (try self.numberValue(&self.parent_tree, key, 0)) orelse return error.InvalidPdfStructure;
            const array = try self.numberObject(&raw);
            if (array != .array or array.array.len > self.max_nodes) return error.PdfStructureLimitExceeded;
            try self.containers.put(self.alloc, key, array);
        }
        const array = self.containers.get(key).?.array;
        const index = std.math.cast(usize, mcid) orelse return error.InvalidPdfStructure;
        if (index >= array.len) return error.InvalidPdfStructure;
        if (array[index] == .null) return null;
        const selected = (try self.owner(try reference(&array[index]), 0, true)) orelse return null;
        if (!self.elements.get(selected).?.bindings.contains(.{ .container = key, .mcid = mcid })) return error.InvalidPdfStructure;
        return .{ .owner = selected, .text = self.elements.get(selected).?.text.? };
    }

    pub fn get(self: *Lookup, container: ?i64, mcid: i64) !?Replacement {
        try self.resolver.check(self.resolver.context);
        // Cached owners require no new work or allocation. Keep honoring them
        // after an unrelated lookup exhausts a limit, so later segments of a
        // known replacement cannot leak their original glyphs back into text.
        return self.getImpl(container, mcid) catch |err| self.failed(err);
    }

    pub fn getObject(self: *Lookup, container: ?i64, raw: *const Object) !?Replacement {
        try self.resolver.check(self.resolver.context);
        const value = (self.optionalValue(raw) catch |err| return self.failed(err)) orelse return null;
        const mcid = value.asInteger() orelse return self.failed(error.InvalidPdfStructure);
        return self.get(container, mcid);
    }

    fn failed(self: *Lookup, err: anyerror) !?Replacement {
        try self.noteFailure(err);
        return null;
    }

    fn noteFailure(self: *Lookup, err: anyerror) !void {
        switch (err) {
            error.Canceled => return err,
            error.OutOfMemory => {
                if (!self.resolver.exhausted(self.resolver.context)) return err;
                self.incomplete = true;
                return;
            },
            else => {
                self.incomplete = true;
                return;
            },
        }
    }
};
