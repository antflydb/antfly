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

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { threadingMode } from "../src/abi.js";
import { createWithOptions, openWithOptions } from "../src/database.js";
import { InvalidArgumentError } from "../src/errors.js";
import { THREADING_SERIALIZED } from "../src/types.js";
import { describeWithLibrary } from "./helpers.js";

function tempDbPath(name: string): string {
  const dir = mkdtempSync(join(tmpdir(), `antfly-lite-${name}-`));
  return join(dir, `${name}.aflite`);
}

describeWithLibrary("concurrency", () => {
  it("threadingMode() and capabilities().threading report serialized", async () => {
    expect(threadingMode()).toBe(THREADING_SERIALIZED);
    const db = await createWithOptions(tempDbPath("threading"), { noSync: true });
    try {
      const caps = await db.capabilities();
      expect(caps.threading).toBe("serialized");
    } finally {
      await db.close();
    }
  });

  it("mixes concurrent batch/search/scan/lookup/stats/runUntilIdle/deleteIndex on one handle without errors", async () => {
    const db = await createWithOptions(tempDbPath("concurrent"), { noSync: true });
    try {
      const writers = 4;
      const writesPerWriter = 20;
      let timestamp = 0n;
      const nextTimestamp = () => ++timestamp;

      const writerTasks = Array.from({ length: writers }, (_, w) =>
        (async () => {
          for (let i = 0; i < writesPerWriter; i++) {
            const key = `doc:w${w}:${i}`;
            await db.batch(
              [{ key, value: { body: `concurrent writer ${w} item ${i}` } }],
              nextTimestamp()
            );
          }
        })()
      );

      let stop = false;
      const readerTask = (async () => {
        while (!stop) {
          await db.search({
            full_text_search: { match: { field: "body", text: "concurrent writer" } },
            limit: 5,
          });
          await db.stats();
          await db.scan({ from: "doc:w", to: "doc:x", include_documents: true, limit: 20 });
          try {
            await db.lookup("doc:w0:0");
          } catch (err) {
            if (!(err instanceof Error) || err.name !== "NotFoundError") throw err;
          }
        }
      })();

      const maintTask = (async () => {
        while (!stop) {
          await db.runUntilIdle();
          await db.deleteIndex("no_such_index");
        }
      })();

      await Promise.all(writerTasks);
      stop = true;
      await Promise.all([readerTask, maintTask]);

      await db.runUntilIdle();
      for (let w = 0; w < writers; w++) {
        for (let i = 0; i < writesPerWriter; i++) {
          const key = `doc:w${w}:${i}`;
          const got = (await db.lookup(key)) as { body: string };
          expect(got.body).toContain(`item ${i}`);
        }
      }
    } finally {
      await db.close();
    }
  }, 20_000);

  it("close() waits for in-flight calls; calls racing or following it fail cleanly", async () => {
    const db = await createWithOptions(tempDbPath("close-race"), { noSync: true });
    await db.batch([{ key: "doc:close", value: { body: "close race" } }], 1);

    let closedSeen = 0;
    const readers = Array.from({ length: 8 }, () =>
      (async () => {
        for (;;) {
          try {
            await db.lookup("doc:close");
          } catch (err) {
            if (err instanceof InvalidArgumentError) {
              closedSeen++;
              return;
            }
            throw err;
          }
        }
      })()
    );

    await new Promise((resolve) => setTimeout(resolve, 20));
    await Promise.all([db.close(), db.close(), db.close()]);
    await Promise.all(readers);

    expect(closedSeen).toBe(8);
    await expect(db.stats()).rejects.toBeInstanceOf(InvalidArgumentError);
  });

  it.each([
    0, 150, 10_000,
  ])("idle connections coexist with busyTimeoutMs %i", async (busyTimeoutMs) => {
    const path = tempDbPath("independent-connections");
    const first = await createWithOptions(path, { noSync: true });
    const second = await openWithOptions(path, { noSync: true, busyTimeoutMs });
    try {
      await first.batch([{ key: "doc:first", value: { body: "from first" } }], 1);
      expect(await second.lookup("doc:first")).toMatchObject({ body: "from first" });
      await second.batch([{ key: "doc:second", value: { body: "from second" } }], 2);
      expect(await first.lookup("doc:second")).toMatchObject({ body: "from second" });
      await first.close();
      expect(await second.lookup("doc:first")).toMatchObject({ body: "from first" });
    } finally {
      await first.close();
      await second.close();
    }
  });

  it("concurrent async searches overlap instead of blocking the event loop", async () => {
    const db = await createWithOptions(tempDbPath("overlap"), { noSync: true });
    try {
      const docs = Array.from({ length: 200 }, (_, i) => ({
        key: `doc:overlap:${i}`,
        value: { body: `overlap search corpus entry number ${i} with some extra words to search` },
      }));
      await db.batch(docs, 1);
      await db.runUntilIdle();

      let ticks = 0;
      const ticker = setInterval(() => {
        ticks++;
      }, 1);

      const searches = Array.from({ length: 16 }, () =>
        db.search({
          full_text_search: { match: { field: "body", text: "overlap search" } },
          limit: 50,
        })
      );
      await Promise.all(searches);
      clearInterval(ticker);

      // If the native calls blocked the event loop, the 1ms interval would
      // never have fired while they were outstanding.
      expect(ticks).toBeGreaterThan(0);
    } finally {
      await db.close();
    }
  });
});
