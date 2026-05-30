// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Freestanding / no-libc stub for the pthread surface used by the Zig LMDB
//! port's writer-lock helper. Zig 0.17 removed the `@cImport` builtin, so the
//! libc-backed bindings now come from a build-system `addTranslateC` module
//! (`lmdb_pthread`); this file is the non-libc fallback wired in its place. The
//! Zig backend never exercises the writer lock, so these are unreachable.

pub const pthread_mutex_t = usize;
pub const pthread_cond_t = usize;

pub fn pthread_mutex_init(_: *pthread_mutex_t, _: ?*anyopaque) c_int {
    unreachable;
}
pub fn pthread_mutex_destroy(_: *pthread_mutex_t) c_int {
    unreachable;
}
pub fn pthread_mutex_lock(_: *pthread_mutex_t) c_int {
    unreachable;
}
pub fn pthread_mutex_unlock(_: *pthread_mutex_t) c_int {
    unreachable;
}
pub fn pthread_cond_init(_: *pthread_cond_t, _: ?*anyopaque) c_int {
    unreachable;
}
pub fn pthread_cond_destroy(_: *pthread_cond_t) c_int {
    unreachable;
}
pub fn pthread_cond_wait(_: *pthread_cond_t, _: *pthread_mutex_t) c_int {
    unreachable;
}
pub fn pthread_cond_signal(_: *pthread_cond_t) c_int {
    unreachable;
}
pub fn pthread_cond_broadcast(_: *pthread_cond_t) c_int {
    unreachable;
}
