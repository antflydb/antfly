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

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const DefinitionMap = std.StringArrayHashMapUnmanaged([][]u8);

const Section = enum {
    none,
    request_definition,
    policy_definition,
    role_definition,
    matchers,
};

pub const Model = struct {
    alloc: Allocator,
    request_definitions: DefinitionMap = .{},
    policy_definitions: DefinitionMap = .{},
    role_definitions: DefinitionMap = .{},
    matchers: DefinitionMap = .{},

    pub fn fromString(alloc: Allocator, raw: []const u8) !Model {
        var model = Model{ .alloc = alloc };
        errdefer model.deinit();

        var current_section: Section = .none;

        var lines = std.mem.tokenizeScalar(u8, raw, '\n');
        while (lines.next()) |line_raw| {
            const line = std.mem.trim(u8, line_raw, " \t\r");
            if (line.len == 0 or line[0] == '#') continue;
            if (line[0] == '[' and line[line.len - 1] == ']') {
                current_section = parseSection(line[1 .. line.len - 1]) orelse return error.UnsupportedModelSection;
                continue;
            }

            const eq = std.mem.indexOfScalar(u8, line, '=') orelse return error.InvalidModelLine;
            const key = std.mem.trim(u8, line[0..eq], " \t");
            const value = std.mem.trim(u8, line[eq + 1 ..], " \t");
            switch (current_section) {
                .request_definition => try putTokens(&model.request_definitions, alloc, key, value),
                .policy_definition => try putTokens(&model.policy_definitions, alloc, key, value),
                .role_definition => try putTokens(&model.role_definitions, alloc, key, value),
                .matchers => try putLiteral(&model.matchers, alloc, key, value),
                .none => return error.InvalidModelLine,
            }
        }

        try model.validate();
        return model;
    }

    pub fn deinit(self: *Model) void {
        deinitDefinitionMap(self.alloc, &self.request_definitions);
        deinitDefinitionMap(self.alloc, &self.policy_definitions);
        deinitDefinitionMap(self.alloc, &self.role_definitions);
        deinitDefinitionMap(self.alloc, &self.matchers);
        self.* = undefined;
    }

    fn validate(self: *const Model) !void {
        const request = self.request_definitions.get("r") orelse return error.MissingRequestDefinition;
        const policy = self.policy_definitions.get("p") orelse return error.MissingPolicyDefinition;
        const grouping = self.role_definitions.get("g") orelse return error.MissingRoleDefinition;
        const matcher = self.matchers.get("m") orelse return error.MissingMatcherDefinition;
        if (!tokensEqual(request, &.{ "sub", "typ", "obj", "act" })) return error.UnsupportedRequestDefinition;
        if (!tokensEqual(policy, &.{ "sub", "typ", "obj", "act" })) return error.UnsupportedPolicyDefinition;
        if (!tokensEqual(grouping, &.{ "_", "_" })) return error.UnsupportedRoleDefinition;
        if (matcher.len != 1) return error.UnsupportedMatcherDefinition;
    }
};

pub const Rule = struct {
    ptype: []u8,
    fields: [][]u8,

    pub fn initOwned(alloc: Allocator, ptype: []const u8, fields: []const []const u8) !Rule {
        const owned_ptype = try alloc.dupe(u8, ptype);
        errdefer alloc.free(owned_ptype);
        const owned_fields = try alloc.alloc([]u8, fields.len);
        errdefer alloc.free(owned_fields);
        var filled: usize = 0;
        errdefer {
            for (owned_fields[0..filled]) |field| alloc.free(field);
        }
        for (fields, 0..) |field, i| {
            owned_fields[i] = try alloc.dupe(u8, field);
            filled += 1;
        }
        return .{
            .ptype = owned_ptype,
            .fields = owned_fields,
        };
    }

    pub fn clone(self: Rule, alloc: Allocator) !Rule {
        return try initOwned(alloc, self.ptype, self.fieldSlice());
    }

    pub fn deinit(self: *Rule, alloc: Allocator) void {
        alloc.free(self.ptype);
        for (self.fields) |field| alloc.free(field);
        alloc.free(self.fields);
        self.* = undefined;
    }

    pub fn fieldSlice(self: Rule) []const []const u8 {
        return @ptrCast(self.fields);
    }
};

pub const Adapter = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        load_policies: *const fn (ptr: *anyopaque, alloc: Allocator) anyerror![]Rule,
        add_policies: *const fn (ptr: *anyopaque, alloc: Allocator, rules: []const Rule) anyerror!void,
        remove_policies: *const fn (ptr: *anyopaque, rules: []const Rule) anyerror!void,
        remove_filtered_policy: *const fn (ptr: *anyopaque, ptype: []const u8, field_index: usize, field_values: []const []const u8) anyerror!usize,
    };

    pub fn loadPolicies(self: Adapter, alloc: Allocator) ![]Rule {
        return try self.vtable.load_policies(self.ptr, alloc);
    }

    pub fn addPolicies(self: Adapter, alloc: Allocator, rules: []const Rule) !void {
        return try self.vtable.add_policies(self.ptr, alloc, rules);
    }

    pub fn removePolicies(self: Adapter, rules: []const Rule) !void {
        return try self.vtable.remove_policies(self.ptr, rules);
    }

    pub fn removeFilteredPolicy(self: Adapter, ptype: []const u8, field_index: usize, field_values: []const []const u8) !usize {
        return try self.vtable.remove_filtered_policy(self.ptr, ptype, field_index, field_values);
    }
};

pub const MemoryAdapter = struct {
    alloc: Allocator,
    rules: std.ArrayList(Rule) = .empty,

    const iface_vtable: Adapter.VTable = .{
        .load_policies = loadPolicies,
        .add_policies = addPolicies,
        .remove_policies = removePolicies,
        .remove_filtered_policy = removeFilteredPolicy,
    };

    pub fn init(alloc: Allocator) MemoryAdapter {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryAdapter) void {
        for (self.rules.items) |*rule| rule.deinit(self.alloc);
        self.rules.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn iface(self: *MemoryAdapter) Adapter {
        return .{
            .ptr = self,
            .vtable = &iface_vtable,
        };
    }

    fn loadPolicies(ptr: *anyopaque, alloc: Allocator) ![]Rule {
        const self: *MemoryAdapter = @ptrCast(@alignCast(ptr));
        const out = try alloc.alloc(Rule, self.rules.items.len);
        errdefer alloc.free(out);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |*rule| rule.deinit(alloc);
        }
        for (self.rules.items, 0..) |rule, i| {
            out[i] = try rule.clone(alloc);
            filled += 1;
        }
        return out;
    }

    fn addPolicies(ptr: *anyopaque, alloc: Allocator, rules: []const Rule) !void {
        const self: *MemoryAdapter = @ptrCast(@alignCast(ptr));
        _ = alloc;
        for (rules) |rule| {
            if (self.contains(rule)) continue;
            try self.rules.append(self.alloc, try rule.clone(self.alloc));
        }
    }

    fn removePolicies(ptr: *anyopaque, rules: []const Rule) !void {
        const self: *MemoryAdapter = @ptrCast(@alignCast(ptr));
        var i: usize = 0;
        while (i < self.rules.items.len) {
            var should_remove = false;
            for (rules) |rule| {
                if (rulesEqual(self.rules.items[i], rule)) {
                    should_remove = true;
                    break;
                }
            }
            if (!should_remove) {
                i += 1;
                continue;
            }
            self.rules.items[i].deinit(self.alloc);
            _ = self.rules.swapRemove(i);
        }
    }

    fn removeFilteredPolicy(ptr: *anyopaque, ptype: []const u8, field_index: usize, field_values: []const []const u8) !usize {
        const self: *MemoryAdapter = @ptrCast(@alignCast(ptr));
        var removed: usize = 0;
        var i: usize = 0;
        while (i < self.rules.items.len) {
            if (!ruleMatchesFilter(self.rules.items[i], ptype, field_index, field_values)) {
                i += 1;
                continue;
            }
            self.rules.items[i].deinit(self.alloc);
            _ = self.rules.swapRemove(i);
            removed += 1;
        }
        return removed;
    }

    fn contains(self: *const MemoryAdapter, rule: Rule) bool {
        for (self.rules.items) |existing| {
            if (rulesEqual(existing, rule)) return true;
        }
        return false;
    }
};

pub const Enforcer = struct {
    alloc: Allocator,
    model: Model,
    adapter: Adapter,
    policies: std.ArrayList(Rule) = .empty,
    auto_save: bool = false,

    pub fn init(alloc: Allocator, model: Model, adapter: Adapter) !Enforcer {
        const loaded = try adapter.loadPolicies(alloc);
        var policies = std.ArrayList(Rule).empty;
        errdefer {
            for (loaded) |*rule| rule.deinit(alloc);
            alloc.free(loaded);
        }
        for (loaded) |rule| try policies.append(alloc, rule);
        alloc.free(loaded);
        return .{
            .alloc = alloc,
            .model = model,
            .adapter = adapter,
            .policies = policies,
        };
    }

    pub fn deinit(self: *Enforcer) void {
        self.model.deinit();
        for (self.policies.items) |*rule| rule.deinit(self.alloc);
        self.policies.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn enableAutoSave(self: *Enforcer, enabled: bool) void {
        self.auto_save = enabled;
    }

    pub fn addPolicies(self: *Enforcer, rules: []const []const []const u8) !bool {
        return try self.addNamedPolicies("p", rules);
    }

    pub fn addPolicy(self: *Enforcer, fields: []const []const u8) !bool {
        return try self.addNamedPolicy("p", fields);
    }

    pub fn addNamedPolicy(self: *Enforcer, ptype: []const u8, fields: []const []const u8) !bool {
        return try self.addNamedPolicies(ptype, &.{fields});
    }

    pub fn addNamedPolicies(self: *Enforcer, ptype: []const u8, rules: []const []const []const u8) !bool {
        var added_any = false;
        var new_rules = std.ArrayList(Rule).empty;
        defer {
            for (new_rules.items) |*rule| rule.deinit(self.alloc);
            new_rules.deinit(self.alloc);
        }

        for (rules) |fields| {
            const rule = try Rule.initOwned(self.alloc, ptype, fields);
            if (self.contains(rule)) {
                var owned = rule;
                owned.deinit(self.alloc);
                continue;
            }
            try self.policies.append(self.alloc, try rule.clone(self.alloc));
            try new_rules.append(self.alloc, rule);
            added_any = true;
        }

        if (added_any and self.auto_save) try self.adapter.addPolicies(self.alloc, new_rules.items);
        return added_any;
    }

    pub fn removeFilteredPolicy(self: *Enforcer, field_index: usize, field_values: []const []const u8) !bool {
        return try self.removeFilteredNamedPolicy("p", field_index, field_values);
    }

    pub fn removeFilteredGroupingPolicy(self: *Enforcer, field_index: usize, field_values: []const []const u8) !bool {
        return try self.removeFilteredNamedPolicy("g", field_index, field_values);
    }

    pub fn removeFilteredNamedPolicy(self: *Enforcer, ptype: []const u8, field_index: usize, field_values: []const []const u8) !bool {
        var removed_any = false;
        var i: usize = 0;
        while (i < self.policies.items.len) {
            if (!ruleMatchesFilter(self.policies.items[i], ptype, field_index, field_values)) {
                i += 1;
                continue;
            }
            self.policies.items[i].deinit(self.alloc);
            _ = self.policies.swapRemove(i);
            removed_any = true;
        }
        if (removed_any and self.auto_save) _ = try self.adapter.removeFilteredPolicy(ptype, field_index, field_values);
        return removed_any;
    }

    pub fn enforce(self: *const Enforcer, sub: []const u8, typ: []const u8, obj: []const u8, act: []const u8) !bool {
        for (self.policies.items) |rule| {
            if (!std.mem.eql(u8, rule.ptype, "p")) continue;
            if (rule.fields.len < 4) continue;
            if (!try self.subjectMatches(sub, rule.fields[0])) continue;
            if (!matchField(typ, rule.fields[1])) continue;
            if (!matchField(obj, rule.fields[2])) continue;
            if (!matchField(act, rule.fields[3])) continue;
            return true;
        }
        return false;
    }

    pub fn getPermissionsForUser(self: *const Enforcer, alloc: Allocator, username: []const u8) ![]Rule {
        var out = std.ArrayList(Rule).empty;
        errdefer {
            for (out.items) |*rule| rule.deinit(alloc);
            out.deinit(alloc);
        }
        for (self.policies.items) |rule| {
            if (!std.mem.eql(u8, rule.ptype, "p")) continue;
            if (!try self.subjectMatches(username, rule.fields[0])) continue;
            try out.append(alloc, try rule.clone(alloc));
        }
        return try out.toOwnedSlice(alloc);
    }

    pub fn getFilteredNamedPolicy(
        self: *const Enforcer,
        alloc: Allocator,
        ptype: []const u8,
        field_index: usize,
        field_values: []const []const u8,
    ) ![]Rule {
        var out = std.ArrayList(Rule).empty;
        errdefer {
            for (out.items) |*rule| rule.deinit(alloc);
            out.deinit(alloc);
        }
        for (self.policies.items) |rule| {
            if (!ruleMatchesFilter(rule, ptype, field_index, field_values)) continue;
            try out.append(alloc, try rule.clone(alloc));
        }
        return try out.toOwnedSlice(alloc);
    }

    fn contains(self: *const Enforcer, rule: Rule) bool {
        for (self.policies.items) |existing| {
            if (rulesEqual(existing, rule)) return true;
        }
        return false;
    }

    fn subjectMatches(self: *const Enforcer, subject: []const u8, policy_subject: []const u8) !bool {
        if (std.mem.eql(u8, subject, policy_subject)) return true;
        var queue = std.ArrayList([]const u8).empty;
        defer queue.deinit(self.alloc);
        var visited = std.StringHashMapUnmanaged(void){};
        defer {
            var it = visited.iterator();
            while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
            visited.deinit(self.alloc);
        }

        try queue.append(self.alloc, subject);
        try visited.put(self.alloc, try self.alloc.dupe(u8, subject), {});
        var index: usize = 0;
        while (index < queue.items.len) : (index += 1) {
            const current = queue.items[index];
            for (self.policies.items) |rule| {
                if (!std.mem.eql(u8, rule.ptype, "g")) continue;
                if (rule.fields.len < 2) continue;
                if (!std.mem.eql(u8, current, rule.fields[0])) continue;
                if (std.mem.eql(u8, rule.fields[1], policy_subject)) return true;
                if (visited.contains(rule.fields[1])) continue;
                const owned = try self.alloc.dupe(u8, rule.fields[1]);
                try visited.put(self.alloc, owned, {});
                try queue.append(self.alloc, rule.fields[1]);
            }
        }
        return false;
    }
};

fn putTokens(map: *DefinitionMap, alloc: Allocator, key: []const u8, value: []const u8) !void {
    const tokens = try parseTokens(alloc, value);
    errdefer freeTokens(alloc, tokens);
    const gop = try map.getOrPut(alloc, key);
    if (gop.found_existing) return error.DuplicateDefinition;
    gop.key_ptr.* = try alloc.dupe(u8, key);
    gop.value_ptr.* = tokens;
}

fn putLiteral(map: *DefinitionMap, alloc: Allocator, key: []const u8, value: []const u8) !void {
    const tokens = try alloc.alloc([]u8, 1);
    errdefer alloc.free(tokens);
    tokens[0] = try alloc.dupe(u8, value);
    errdefer alloc.free(tokens[0]);
    const gop = try map.getOrPut(alloc, key);
    if (gop.found_existing) return error.DuplicateDefinition;
    gop.key_ptr.* = try alloc.dupe(u8, key);
    gop.value_ptr.* = tokens;
}

fn parseSection(name: []const u8) ?Section {
    if (std.mem.eql(u8, name, "request_definition")) return .request_definition;
    if (std.mem.eql(u8, name, "policy_definition")) return .policy_definition;
    if (std.mem.eql(u8, name, "role_definition")) return .role_definition;
    if (std.mem.eql(u8, name, "matchers")) return .matchers;
    return null;
}

fn parseTokens(alloc: Allocator, value: []const u8) ![][]u8 {
    var parts = std.ArrayList([]u8).empty;
    errdefer {
        for (parts.items) |part| alloc.free(part);
        parts.deinit(alloc);
    }
    var iter = std.mem.tokenizeAny(u8, value, ", ");
    while (iter.next()) |part| {
        if (part.len == 0) continue;
        try parts.append(alloc, try alloc.dupe(u8, part));
    }
    return try parts.toOwnedSlice(alloc);
}

fn freeTokens(alloc: Allocator, tokens: [][]u8) void {
    for (tokens) |token| alloc.free(token);
    alloc.free(tokens);
}

fn deinitDefinitionMap(alloc: Allocator, map: *DefinitionMap) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(entry.key_ptr.*);
        freeTokens(alloc, entry.value_ptr.*);
    }
    map.deinit(alloc);
}

fn tokensEqual(actual: [][]u8, expected: []const []const u8) bool {
    if (actual.len != expected.len) return false;
    for (actual, expected) |lhs, rhs| {
        if (!std.mem.eql(u8, lhs, rhs)) return false;
    }
    return true;
}

fn rulesEqual(lhs: Rule, rhs: Rule) bool {
    if (!std.mem.eql(u8, lhs.ptype, rhs.ptype)) return false;
    if (lhs.fields.len != rhs.fields.len) return false;
    for (lhs.fields, rhs.fields) |lhs_field, rhs_field| {
        if (!std.mem.eql(u8, lhs_field, rhs_field)) return false;
    }
    return true;
}

fn ruleMatchesFilter(rule: Rule, ptype: []const u8, field_index: usize, field_values: []const []const u8) bool {
    if (!std.mem.eql(u8, rule.ptype, ptype)) return false;
    for (field_values, 0..) |value, i| {
        if (value.len == 0) continue;
        const idx = field_index + i;
        if (idx >= rule.fields.len) return false;
        if (!std.mem.eql(u8, rule.fields[idx], value)) return false;
    }
    return true;
}

fn matchField(requested: []const u8, policy: []const u8) bool {
    return std.mem.eql(u8, policy, "*") or std.mem.eql(u8, requested, policy);
}

test "model parses Go usermgr RBAC model" {
    const alloc = std.testing.allocator;
    const raw =
        \\[request_definition]
        \\r = sub, typ, obj, act
        \\
        \\[policy_definition]
        \\p = sub, typ, obj, act
        \\p2 = sub, obj, filter
        \\
        \\[role_definition]
        \\g = _, _
        \\
        \\[matchers]
        \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
    ;
    var model = try Model.fromString(alloc, raw);
    defer model.deinit();
    try std.testing.expect(model.policy_definitions.contains("p"));
    try std.testing.expect(model.policy_definitions.contains("p2"));
    try std.testing.expect(model.role_definitions.contains("g"));
}

test "enforcer matches direct and grouped policies" {
    const alloc = std.testing.allocator;
    const raw =
        \\[request_definition]
        \\r = sub, typ, obj, act
        \\[policy_definition]
        \\p = sub, typ, obj, act
        \\[role_definition]
        \\g = _, _
        \\[matchers]
        \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
    ;
    var adapter = MemoryAdapter.init(alloc);
    defer adapter.deinit();
    var enforcer = try Enforcer.init(alloc, try Model.fromString(alloc, raw), adapter.iface());
    defer enforcer.deinit();
    enforcer.enableAutoSave(true);

    try std.testing.expect(try enforcer.addPolicies(&.{
        &.{ "alice", "table", "docs", "read" },
        &.{ "reader", "table", "docs", "write" },
    }));
    try std.testing.expect(try enforcer.addNamedPolicies("g", &.{
        &.{ "bob", "reader" },
    }));

    try std.testing.expect(try enforcer.enforce("alice", "table", "docs", "read"));
    try std.testing.expect(!(try enforcer.enforce("alice", "table", "docs", "write")));
    try std.testing.expect(try enforcer.enforce("bob", "table", "docs", "write"));
}

test "enforcer remove filtered policy mirrors Go user deletion shape" {
    const alloc = std.testing.allocator;
    const raw =
        \\[request_definition]
        \\r = sub, typ, obj, act
        \\[policy_definition]
        \\p = sub, typ, obj, act
        \\[role_definition]
        \\g = _, _
        \\[matchers]
        \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
    ;
    var adapter = MemoryAdapter.init(alloc);
    defer adapter.deinit();
    var enforcer = try Enforcer.init(alloc, try Model.fromString(alloc, raw), adapter.iface());
    defer enforcer.deinit();
    enforcer.enableAutoSave(true);

    _ = try enforcer.addPolicies(&.{
        &.{ "alice", "table", "docs", "read" },
        &.{ "alice", "table", "logs", "write" },
        &.{ "bob", "table", "docs", "read" },
    });
    try std.testing.expect(try enforcer.removeFilteredPolicy(0, &.{"alice"}));
    try std.testing.expect(!(try enforcer.enforce("alice", "table", "docs", "read")));
    try std.testing.expect(try enforcer.enforce("bob", "table", "docs", "read"));
}

test "enforcer returns Go-shaped permission tuples" {
    const alloc = std.testing.allocator;
    const raw =
        \\[request_definition]
        \\r = sub, typ, obj, act
        \\[policy_definition]
        \\p = sub, typ, obj, act
        \\[role_definition]
        \\g = _, _
        \\[matchers]
        \\m = g(r.sub, p.sub) && (r.typ == p.typ || p.typ == "*") && (r.obj == p.obj || p.obj == "*") && (r.act == p.act || p.act == "*")
    ;
    var adapter = MemoryAdapter.init(alloc);
    defer adapter.deinit();
    var enforcer = try Enforcer.init(alloc, try Model.fromString(alloc, raw), adapter.iface());
    defer enforcer.deinit();
    _ = try enforcer.addPolicies(&.{
        &.{ "alice", "table", "docs", "read" },
    });
    const permissions = try enforcer.getPermissionsForUser(alloc, "alice");
    defer {
        for (permissions) |*rule| rule.deinit(alloc);
        alloc.free(permissions);
    }
    try std.testing.expectEqual(@as(usize, 1), permissions.len);
    try std.testing.expectEqualStrings("p", permissions[0].ptype);
    try std.testing.expectEqualStrings("alice", permissions[0].fields[0]);
    try std.testing.expectEqualStrings("table", permissions[0].fields[1]);
    try std.testing.expectEqualStrings("docs", permissions[0].fields[2]);
    try std.testing.expectEqualStrings("read", permissions[0].fields[3]);
}
