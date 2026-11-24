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

//! An owned tokenizer with an optional retained resource-domain capability.
//! Registry and eviction policy stay with the service that supplied the ticket.
const hf_tokenizer = @import("inference_hf_tokenizer");
const memory = @import("../runtime/tier/memory.zig");

pub const Lifetime = struct {
    context: *anyopaque,
    release_fn: *const fn (*anyopaque) void,
    admission_fn: *const fn (*anyopaque) ?memory.AdmissionLease,
};

pub const Managed = struct {
    tokenizer: *hf_tokenizer.HfTokenizer,
    resource_lease: ?memory.AdmissionLease = null,
    managed_lifetime: ?Lifetime = null,
    owns_tokenizer: bool = true,

    pub fn reclaimableAdmission(self: *const Managed) ?memory.AdmissionLease {
        if (self.resource_lease) |lease| return lease;
        const capability = self.managed_lifetime orelse return null;
        return capability.admission_fn(capability.context);
    }

    pub fn deinit(self: *Managed) void {
        if (self.owns_tokenizer) self.tokenizer.deinitSelf();
        if (self.resource_lease) |*lease| lease.release();
        if (self.managed_lifetime) |managed| managed.release_fn(managed.context);
        self.resource_lease = null;
        self.managed_lifetime = null;
        self.owns_tokenizer = false;
    }

    pub fn take(self: *Managed) Managed {
        const owned = self.*;
        self.resource_lease = null;
        self.managed_lifetime = null;
        self.owns_tokenizer = false;
        return owned;
    }
};
