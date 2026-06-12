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

pub const auth = @import("auth.zig");

pub const default_scope = auth.default_scope;
pub const default_token_uri = auth.default_token_uri;
pub const HeaderPair = auth.HeaderPair;
pub const HttpMethod = auth.HttpMethod;
pub const TransportResponse = auth.TransportResponse;
pub const ServiceAccount = auth.ServiceAccount;
pub const Config = auth.Config;
pub const CachedTokenSource = auth.CachedTokenSource;
pub const configFromServiceAccountAlloc = auth.configFromServiceAccountAlloc;
pub const parseServiceAccountJsonAlloc = auth.parseServiceAccountJsonAlloc;
pub const serviceAccountFromFileAlloc = auth.serviceAccountFromFileAlloc;
pub const tokenSourceFromEnvAlloc = auth.tokenSourceFromEnvAlloc;
pub const configFromEnvAlloc = auth.configFromEnvAlloc;
pub const serviceAccountEnvProjectIdAlloc = auth.serviceAccountEnvProjectIdAlloc;
pub const signedJwtAssertionAlloc = auth.signedJwtAssertionAlloc;

test "google module compiles" {
    _ = auth;
    _ = default_scope;
    _ = default_token_uri;
    _ = HeaderPair;
    _ = HttpMethod;
    _ = TransportResponse;
    _ = ServiceAccount;
    _ = Config;
    _ = CachedTokenSource;
}
