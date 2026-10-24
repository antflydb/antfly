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

import { expect, it } from "vitest";
import { memoryJsonSlice, parseMemoryJson } from "../src/marshal.js";

it("round-trips full-width native memory integers through the C ABI envelope", () => {
  const decoded = parseMemoryJson(
    Buffer.from(
      '{"resource":{"table_id":18446744073709551615,"revision":9007199254740993},"small":3,"fraction":0.25,"id":"9007199254740993"}'
    )
  );
  expect(decoded).toEqual({
    resource: { table_id: 18446744073709551615n, revision: 9007199254740993n },
    small: 3,
    fraction: 0.25,
    id: "9007199254740993",
  });
  const slice = memoryJsonSlice({ action: "explain", body: decoded });
  expect(Buffer.from(slice.ptr as Buffer).toString("utf8")).toContain(
    '"revision":9007199254740993'
  );
  expect(() => memoryJsonSlice({ revision: Number("9007199254740993") })).toThrow(
    /already lost precision/
  );
});
