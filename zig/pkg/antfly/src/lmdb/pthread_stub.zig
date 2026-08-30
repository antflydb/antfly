//! Freestanding/no-libc placeholder for the pthread surface used by LMDB.

pub const pthread_mutex_t = usize;
pub const pthread_cond_t = usize;

pub fn pthread_mutex_init(_: *pthread_mutex_t, _: ?*anyopaque) c_int { unreachable; }
pub fn pthread_mutex_destroy(_: *pthread_mutex_t) c_int { unreachable; }
pub fn pthread_mutex_lock(_: *pthread_mutex_t) c_int { unreachable; }
pub fn pthread_mutex_unlock(_: *pthread_mutex_t) c_int { unreachable; }
pub fn pthread_cond_init(_: *pthread_cond_t, _: ?*anyopaque) c_int { unreachable; }
pub fn pthread_cond_destroy(_: *pthread_cond_t) c_int { unreachable; }
pub fn pthread_cond_wait(_: *pthread_cond_t, _: *pthread_mutex_t) c_int { unreachable; }
pub fn pthread_cond_signal(_: *pthread_cond_t) c_int { unreachable; }
pub fn pthread_cond_broadcast(_: *pthread_cond_t) c_int { unreachable; }
