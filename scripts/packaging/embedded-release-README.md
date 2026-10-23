# Antfly embedding library

This Apache-2.0 archive contains the in-process database and inference runtime:

- `lib/libantfly`: native shared library (`.so` or `.dylib`; Windows uses `bin/antfly.dll`).
- `include/antfly.h`: public C API.
- `lib/pkgconfig/libantfly.pc`: relocatable native build metadata.
- Runtime assets and applicable license notices.

Keep the archive layout together. Add `lib/pkgconfig` to `PKG_CONFIG_PATH` for
C, Go, and Rust consumers. Python and npm embedded packages bundle the same
library. `libantfly` runs inference in-process and needs no CLI or worker.

Install the separate `antfly` CLI for local file commands (`antfly lite`),
inference commands (`antfly inference`), or database serving (`antfly standalone`).
The embedded archive installs no public executables.

First-party Antfly code is Apache-2.0. Third-party components retain the terms
recorded in `LICENSES/third-party/` and `THIRD_PARTY_NOTICES.md`.
