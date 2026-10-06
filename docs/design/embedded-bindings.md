# Embedded bindings and package names

The Apache-2.0 `libantfly` C ABI is the single native runtime for local
databases and inference. Its language bindings use **embedded** as their public
package name, because `Lite` describes the `.aflite` storage format while the
same bindings also open single-node directory storage and run inference without
a database.

| Language | Public package | Import |
| --- | --- | --- |
| Go | `github.com/antflydb/antfly/go/pkg/embedded` | `embedded` |
| Python | `antfly-embedded` | `antfly_embedded` |
| Rust | `antfly-embedded` | `antfly_embedded` |
| TypeScript/Node | `@antfly/embedded` | `@antfly/embedded` |

The Rust `antfly-embedded-sys` crate holds raw C declarations for the safe
`antfly-embedded` crate. npm's `@antfly/embedded-<platform>` packages carry the
same library for the public selector package. These are implementation and
distribution layers, not separate user-facing Lite or inference APIs. Each
public binding exposes both database and database-free inference operations.
The network clients retain their `sdk` names.

The native library remains `libantfly` and its header remains `antfly.h`.
Existing `antfly_lite_*` C symbols are ABI entry points and keep their names.
The `antfly-lite` executable, `.aflite` extension, and Lite CLI commands keep
their names. The Apache distribution is named `antfly-embedded` because it
contains database and inference tools together. Renaming a language package does not
change those native artifacts or split the runtime into separate libraries.

Release packaging produces platform wheels for `antfly-embedded` and an npm
`@antfly/embedded` selector with platform packages from the Apache Embedded
archives. The publishing workflow verifies the exact wheels and npm tarballs
against the archives before registry promotion. Go and Rust consumers link the
same C ABI from a release archive or local build. A separate Lite or inference
convenience package should be introduced only when it offers a distinct API or
distribution benefit; the current combined bindings cover both use cases.

Installing an embedded binding does not register `antfly-lite` or
`antfly-inference` as a command. Python/npm packages include the native
library without a private worker. Bindings call the C API, which runs
inference in-process on every backend, including Metal, CUDA, ONNX, and
PJRT. `libantfly` disables process isolation and does not use
`ANTFLY_INFERENCE_WORKER`. A GPU driver call cannot be interrupted once
entered, and a driver fault can terminate the embedding application.

Prebuilt Python/npm packages support Linux x86-64 and ARM64 (glibc 2.28+)
and macOS ARM64. Windows, Intel macOS, and Alpine/musl have no prebuilt
language packages. Go/Rust/C consumers use the native archives, whose
platform matrix is independent, with relocatable `lib/pkgconfig/libantfly.pc`
metadata. Go uses pkg-config; Rust also retains explicit directory overrides
and a source-checkout fallback. Keep the discovered library installed at runtime.

Server hosts that enable process isolation can use a replaceable inference
worker. The single Apache Embedded archive distributes both public CLIs
(`antfly-lite` and `antfly-inference`), `libantfly`, `antfly.h`, and the private
worker, with runtime files and license notices.
There are two primary release products: the ELv2 server and the Apache embedded
runtime. Source ownership remains separated between the embedded integration
and inference engine packages.

Independent Apache-only Zig fetching is tracked in
[issue #988](https://github.com/antflydb/antfly/issues/988). The package
currently requires a monorepo checkout; a standalone manifest, dependency
closure, and immutable source artifact remain to be implemented.
