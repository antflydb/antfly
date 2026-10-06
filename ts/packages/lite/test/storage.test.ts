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

/**
 * Coverage beyond the shared conformance cases for the storage_kind /
 * cross-storage-restore surface added in the capi/naming-cleanup ABI
 * cleanup: restoring a .aflite backup into directory storage, and reopening
 * that directory afterwards (see zig/pkg/antfly/capi-conformance/cases/
 * directory_storage.json and backup_across_storage.json for the shared
 * cases this complements).
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createWithOptions, openWithOptions } from "../src/database.js";
import { restore } from "../src/files.js";
import { Storage } from "../src/types.js";
import { describeWithLibrary } from "./helpers.js";

describeWithLibrary("restore into directory storage", () => {
  it("restores a .aflite backup into a directory and reopens it", async () => {
    const dir = mkdtempSync(join(tmpdir(), "antfly-lite-storage-"));
    const liteDb = await createWithOptions(join(dir, "source.aflite"), { noSync: true });
    try {
      await liteDb.batch(
        [
          { key: "doc:a", value: { title: "restored into a directory" } },
          { key: "doc:b", value: { title: "second document" } },
        ],
        1
      );
      await liteDb.runUntilIdle();
      const backup = await liteDb.backup();

      const dirPath = join(dir, "restored-dir");
      await restore(dirPath, backup, { storage: Storage.Directory });

      // Reopen the restored directory as a fresh handle and confirm the data
      // round-tripped and status reports the directory storage format.
      const dirDb = await openWithOptions(dirPath, { storage: Storage.Directory, noSync: true });
      try {
        const status = await dirDb.status();
        expect(status.storage.format).toBe("directory");

        const a = (await dirDb.lookup("doc:a")) as { title: string };
        expect(a.title).toBe("restored into a directory");
        const b = (await dirDb.lookup("doc:b")) as { title: string };
        expect(b.title).toBe("second document");
      } finally {
        await dirDb.close();
      }
    } finally {
      await liteDb.close();
    }
  });
});
