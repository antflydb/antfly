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

//! Shared ordered SQL name lookup for transport adapters. Only definite name
//! absence advances the path; authorization and catalog failures are terminal.
const std = @import("std");
const catalog = @import("antfly_local_sources").sql_catalog;
const ast = @import("antfly_local_sources").sql_ast;

pub fn resolveOnPath(native_backend: catalog.Backend, alloc: std.mem.Allocator, name: ast.Name, action: catalog.Action, path: ?*const @import("../pgwire/search_path.zig").Path) !catalog.Table {
    return resolve(false, native_backend, alloc, name, action, path);
}

pub fn resolveReadOnPath(native_backend: catalog.Backend, alloc: std.mem.Allocator, name: ast.Name, path: ?*const @import("../pgwire/search_path.zig").Path) !catalog.ReadRelation {
    return resolve(true, native_backend, alloc, name, .read, path);
}

fn lookup(comptime read: bool, backend: catalog.Backend, a: std.mem.Allocator, name: ast.Name, action: catalog.Action) !if (read) catalog.ReadRelation else catalog.Table {
    return if (read) backend.resolveRead(a, name) else backend.vtable.resolve(backend.ptr, a, name, action);
}

fn resolve(comptime read: bool, backend: catalog.Backend, a: std.mem.Allocator, input: ast.Name, action: catalog.Action, path: ?*const @import("../pgwire/search_path.zig").Path) !if (read) catalog.ReadRelation else catalog.Table {
    var namespace_buffer: [72]u8 = undefined;
    const temporary: ?[]const u8 = if (backend.temporary_relation_owner) |owner| try owner.namespace(&namespace_buffer) else null;
    var name = input;
    if (name.namespace) |namespace| {
        if (temporary != null and std.mem.eql(u8, namespace, "pg_temp")) name.namespace = temporary;
        return lookup(read, backend, a, name, action);
    }
    if (name.database != null) return lookup(read, backend, a, name, action);
    const explicit_temporary = if (path) |selected| for (selected.entries[0..selected.len]) |*entry| {
        if (std.mem.eql(u8, entry.slice(), "pg_temp") or (temporary != null and std.mem.eql(u8, entry.slice(), temporary.?))) break true;
    } else false else false;
    // Explicit pg_temp fixes its position; implicit TEMP precedes ordinary
    // schemas for relations, never functions or operators.
    if (temporary != null and !explicit_temporary) {
        var scoped = name;
        scoped.namespace = temporary;
        if (lookup(read, backend, a, scoped, action)) |value| return value else |err| switch (err) {
            error.TableNotFound => {},
            else => return err,
        }
    }
    if (path == null) return lookup(read, backend, a, name, action);
    for (path.?.entries[0..path.?.len]) |*entry| {
        var scoped = name;
        scoped.namespace = if (temporary != null and std.mem.eql(u8, entry.slice(), "pg_temp")) temporary.? else entry.slice();
        if (lookup(read, backend, a, scoped, action)) |value| return value else |err| switch (err) {
            error.TableNotFound => continue,
            else => return err,
        }
    }
    return error.TableNotFound;
}
