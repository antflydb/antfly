// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

// Package antflylite provides Go bindings for embedded Antfly Lite databases.
//
// The package is backed by the stable Antfly Lite C ABI. Build the C library
// with `zig build lite-capi` before running cgo-backed tests or binaries from
// the source tree.
package antflylite
