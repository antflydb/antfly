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

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

struct native_fd_cache {
    uint64_t layout;
    void *cache;
};

/* Publish the cache and its layout together. The cache is initialized only on
 * the cold path. A concurrent initializer may lose the CAS and discard its
 * unshared candidate, so the anchor needs neither libc nor a startup lock. */
__attribute__((visibility("hidden")))
void *antfly_platform_native_fd_cache_v2(
    uint64_t layout,
    struct native_fd_cache *(*init)(uint64_t),
    void (*discard)(struct native_fd_cache *)
) {
    static _Atomic(struct native_fd_cache *) published;
    struct native_fd_cache *current = atomic_load_explicit(&published, memory_order_acquire);
    if (current == NULL) {
        struct native_fd_cache *candidate = init(layout);
        if (atomic_compare_exchange_strong_explicit(
                &published, &current, candidate, memory_order_acq_rel, memory_order_acquire)) {
            current = candidate;
        } else {
            discard(candidate);
        }
    }
    return current->layout == layout ? current->cache : NULL;
}
