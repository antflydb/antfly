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

// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { describe, it } from "vitest";
import { loadNative } from "../src/native.js";

/**
 * Tests needing the native library skip cleanly when it can't be found,
 * unless ANTFLY_LITE_REQUIRE_LIBRARY=1 is set, in which case they fail.
 */
export const REQUIRE_LIBRARY = process.env.ANTFLY_LITE_REQUIRE_LIBRARY === "1";

interface Availability {
  available: boolean;
  error?: unknown;
}

let cached: Availability | undefined;

export function libraryAvailability(): Availability {
  if (!cached) {
    try {
      loadNative();
      cached = { available: true };
    } catch (err) {
      cached = { available: false, error: err };
    }
  }
  return cached;
}

/**
 * Wraps a describe block that needs the native library: skips cleanly when
 * it isn't found, unless ANTFLY_LITE_REQUIRE_LIBRARY=1, in which case a
 * single failing test reports why.
 */
export function describeWithLibrary(name: string, fn: () => void): void {
  const { available, error } = libraryAvailability();
  if (available) {
    describe(name, fn);
    return;
  }
  if (REQUIRE_LIBRARY) {
    describe(name, () => {
      it("requires the native libantfly library (ANTFLY_LITE_REQUIRE_LIBRARY=1)", () => {
        throw error instanceof Error ? error : new Error(String(error));
      });
    });
    return;
  }
  describe.skip(`${name} (skipped: native libantfly library not found)`, fn);
}
