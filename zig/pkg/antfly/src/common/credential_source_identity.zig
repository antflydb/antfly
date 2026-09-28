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
const credentials = @import("antfly_credentials");
const secrets = @import("secrets.zig");

pub const CredentialSourceIdentity = credentials.CredentialSourceIdentity;
pub const updateField = credentials.updateField;

pub fn fromSecretValue(secret: ?secrets.SecretValue) CredentialSourceIdentity {
    const value = secret orelse return CredentialSourceIdentity.none();
    return switch (value) {
        .literal => |literal| CredentialSourceIdentity.literalSecret(literal),
        .secret_ref => |reference| CredentialSourceIdentity.secretReference(reference),
        .env_var => |name| CredentialSourceIdentity.environmentVariable(name),
    };
}

pub fn testCredentialSourceIdentities() !void {
    const Identity = CredentialSourceIdentity;

    try std.testing.expect(Identity.googleAdc(null).eql(Identity.googleAdc(null)));
    try std.testing.expect(!Identity.googleAdc(null).eql(Identity.googleAdc("<default-adc>")));
    try std.testing.expect(!Identity.googleAdc("credentials-a.json").eql(Identity.googleAdc("credentials-b.json")));
    try std.testing.expect(Identity.awsDefaultChain().eql(Identity.awsDefaultChain()));
    try std.testing.expect(!Identity.awsDefaultChain().eql(Identity.none()));
    try std.testing.expect(!Identity.awsProfile("default", null).eql(Identity.awsProfile("default", "")));
    try std.testing.expect(!Identity.awsWebIdentity("role", "token", "antfly", null).eql(
        Identity.awsWebIdentity("role", "token", "antfly", ""),
    ));

    const literal = secrets.SecretValue{ .literal = @constCast("same") };
    const reference = secrets.SecretValue{ .secret_ref = @constCast("same") };
    const env_var = secrets.SecretValue{ .env_var = @constCast("same") };
    try std.testing.expect(!fromSecretValue(literal).eql(fromSecretValue(reference)));
    try std.testing.expect(!fromSecretValue(reference).eql(fromSecretValue(env_var)));
}

test "credential source identities preserve provider and locator boundaries" {
    try testCredentialSourceIdentities();
}
