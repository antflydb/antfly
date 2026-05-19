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

//! openapi-zig: OpenAPI 3.0.x to Zig code generator
//!
//! Parses OpenAPI JSON specs and generates typed Zig client/server code
//! targeting httpx.zig.

pub const types = @import("types.zig");
pub const parser = @import("parser.zig");
pub const resolver = @import("resolver.zig");
pub const naming = @import("naming.zig");
pub const writer = @import("writer.zig");
pub const codegen = @import("codegen.zig");
pub const codegen_types = @import("codegen_types.zig");
pub const codegen_client = @import("codegen_client.zig");
pub const codegen_server = @import("codegen_server.zig");
pub const codegen_shared = @import("codegen_shared.zig");

pub const OpenApiDoc = types.OpenApiDoc;
pub const Schema = types.Schema;
pub const SchemaOrRef = types.SchemaOrRef;
pub const Operation = types.Operation;
pub const PathItem = types.PathItem;

pub const Parser = parser.Parser;
pub const Resolver = resolver.Resolver;
pub const SourceWriter = writer.SourceWriter;

test {
    _ = types;
    _ = parser;
    _ = resolver;
    _ = naming;
    _ = writer;
    _ = codegen;
    _ = codegen_types;
    _ = codegen_client;
    _ = codegen_server;
    _ = codegen_shared;
}
