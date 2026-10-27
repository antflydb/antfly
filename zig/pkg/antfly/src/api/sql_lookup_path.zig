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
    if (name.namespace != null or name.database != null or path == null) return native_backend.vtable.resolve(native_backend.ptr, alloc, name, action);
    // All candidates use the same native catalog-revision fence. A missing
    // relation may advance; auth, schema churn, and runtime failures may not.
    const selected = path.?;
    for (selected.entries[0..selected.len]) |*entry| {
        var scoped = name;
        scoped.namespace = entry.slice();
        const table = native_backend.vtable.resolve(native_backend.ptr, alloc, scoped, action) catch |err| switch (err) {
            error.TableNotFound => continue,
            else => return err,
        };
        return table;
    }
    return error.TableNotFound;
}

pub fn resolveReadOnPath(native_backend: catalog.Backend, alloc: std.mem.Allocator, name: ast.Name, path: ?*const @import("../pgwire/search_path.zig").Path) !catalog.ReadRelation {
    if (name.namespace != null or name.database != null or path == null) return native_backend.resolveRead(alloc, name);
    const selected = path.?;
    for (selected.entries[0..selected.len]) |*entry| {
        var scoped = name;
        scoped.namespace = entry.slice();
        const relation = native_backend.resolveRead(alloc, scoped) catch |err| switch (err) {
            error.TableNotFound => continue,
            else => return err,
        };
        return relation;
    }
    return error.TableNotFound;
}
