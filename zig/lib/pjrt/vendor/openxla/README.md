`pjrt_c_api.h` is an unmodified upstream OpenXLA header used only by the ABI tests.
Source: https://github.com/openxla/xla/blob/main/xla/pjrt/c/pjrt_c_api.h
Git blob SHA: `6f1f6d4abebb8ab12619fe88b8c8ce4fa8f10f64` (retrieved 2026-11-06).

Keep the header intact when refreshing it. The tests compare every function slot
used by the wrapper and the executable ownership/arity, buffer metadata, and memory layout argument
layouts against
this independent declaration. Runtime builds do not require a C header or XLA SDK.

License: Apache-2.0; the upstream OpenXLA copyright and license notice are retained.
