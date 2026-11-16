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

import { normalTestSuite, testAdapter } from "@better-auth/test-utils/adapter";
import { prepareWorkersAuth, workersAdapter } from "../src/workers.js";
import { MemoryAuthStore } from "./workers-fixture.js";

let store = new MemoryAuthStore();
(
  await testAdapter({
    adapter: () => workersAdapter({ transaction: (fn) => store.transaction(fn) }),
    runMigrations: async (options) => {
      store = new MemoryAuthStore();
      await prepareWorkersAuth(store, options);
    },
    tests: [normalTestSuite()],
  })
).execute();
