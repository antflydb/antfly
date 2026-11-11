// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
import type { Kysely } from "kysely";
import { describe, expect, it, vi } from "vitest";
import {
  type AntflyMigrationClaim,
  type MigrationClaimStore,
  persistentMigrationLock,
} from "../src/kysely-migration-lock.js";

function fixture() {
  let claim: AntflyMigrationClaim | null = null;
  const store: MigrationClaimStore = {
    ensure: vi.fn(async () => {}),
    read: vi.fn(async () => claim),
    insert: vi.fn(async (_db, value) => {
      if (claim) throw { code: "23505" };
      claim = value;
    }),
    remove: vi.fn(async (_db, token) => {
      if (claim?.ownerToken !== token) return false;
      claim = null;
      return true;
    }),
  };
  return { store, db: {} as Kysely<unknown>, claim: () => claim };
}

describe("persistent migration ownership", () => {
  it("waits with reads while another runner owns the claim, then arbitrates atomically", async () => {
    vi.useFakeTimers();
    try {
      const { store, db } = fixture();
      const owner = persistentMigrationLock(store);
      const contender = persistentMigrationLock(store);
      await owner.acquire(db);
      const waiting = contender.acquire(db);
      await vi.advanceTimersByTimeAsync(500);
      // One owner INSERT and one definitively rejected contender INSERT.
      // Repeated waiting must not compete with the owner's DDL for a writer.
      expect(store.insert).toHaveBeenCalledTimes(2);
      expect(vi.mocked(store.read).mock.calls.length).toBeGreaterThan(3);
      await owner.release(db);
      await vi.advanceTimersByTimeAsync(200);
      await waiting;
      expect(store.insert).toHaveBeenCalledTimes(3);
      await contender.release(db);
    } finally {
      vi.useRealTimers();
    }
  });
  it("excludes another runner and only releases confirmed ownership", async () => {
    const { store, db, claim } = fixture();
    const first = persistentMigrationLock(store, { waitTimeoutMs: 0 });
    const second = persistentMigrationLock(store, { waitTimeoutMs: 0 });
    await first.acquire(db);
    const token = claim()?.ownerToken;
    await expect(second.acquire(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_BUSY" });
    await second.release(db);
    expect(claim()?.ownerToken).toBe(token);
    await first.release(db);
    expect(claim()).toBeNull();
  });
  it("reconciles an acknowledged-lost insert through its unique token", async () => {
    const { store, db } = fixture();
    const insert = store.insert;
    store.insert = vi.fn(async (...args) => {
      await insert(...args);
      throw new Error("connection lost");
    });
    const lock = persistentMigrationLock(store);
    await lock.acquire(db);
    await lock.release(db);
    expect(store.insert).toHaveBeenCalledTimes(1);
  });
  it("never retries an uncertain insert or removes an unconfirmed claim", async () => {
    const { store, db } = fixture();
    store.insert = vi.fn(async () => {
      throw new Error("connection lost");
    });
    const lock = persistentMigrationLock(store);
    await expect(lock.acquire(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_UNKNOWN" });
    await lock.release(db);
    expect(store.insert).toHaveBeenCalledTimes(1);
    expect(store.remove).not.toHaveBeenCalled();
  });
  it("preserves a claim when ownership read fails", async () => {
    const { store, db, claim } = fixture();
    store.read = vi.fn(async () => {
      throw new Error("network");
    });
    const lock = persistentMigrationLock(store);
    await expect(lock.acquire(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_UNKNOWN" });
    await lock.release(db);
    expect(claim()).not.toBeNull();
    expect(store.remove).not.toHaveBeenCalled();
  });
  it("reports lost ownership without removing a successor", async () => {
    const { store, db } = fixture();
    const lock = persistentMigrationLock(store);
    await lock.acquire(db);
    store.remove = vi.fn(async () => false);
    await expect(lock.release(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_LOST" });
  });
  it("reports uncertain releases without replaying writes", async () => {
    const { store, db } = fixture();
    const lock = persistentMigrationLock(store);
    await lock.acquire(db);
    store.remove = vi.fn(async () => {
      throw new Error("network");
    });
    await expect(lock.release(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_UNKNOWN" });
    expect(store.remove).toHaveBeenCalledTimes(1);
  });
});

it("retries a read-only schema conflict without repeating the claim insert", async () => {
  const { store, db } = fixture();
  const read = store.read;
  let reads = 0;
  store.read = vi.fn(async (...args) => {
    if (reads++ === 0) throw { code: "40001" };
    return read(...args);
  });
  const lock = persistentMigrationLock(store, { waitTimeoutMs: 1000 });
  await lock.acquire(db);
  expect(store.insert).toHaveBeenCalledTimes(1);
  await lock.release(db);
});

it("retries a definitely rejected serialization conflict after reading ownership", async () => {
  const { store, db } = fixture();
  const insert = store.insert;
  let attempts = 0;
  store.insert = vi.fn(async (...args) => {
    if (attempts++ === 0) throw { code: "40001" };
    return insert(...args);
  });
  const lock = persistentMigrationLock(store, { waitTimeoutMs: 1000 });
  await lock.acquire(db);
  expect(store.read).toHaveBeenCalledTimes(2);
  expect(store.insert).toHaveBeenCalledTimes(2);
  await lock.release(db);
});

it.each([
  "40001",
  "53300",
])("retries a definitely rejected %s release without abandoning the claim", async (code) => {
  const { store, db, claim } = fixture();
  const remove = store.remove;
  let attempts = 0;
  store.remove = vi.fn(async (...args) => {
    if (attempts++ === 0) throw { code };
    return remove(...args);
  });
  const lock = persistentMigrationLock(store, { waitTimeoutMs: 1000 });
  await lock.acquire(db);
  await lock.release(db);
  expect(store.remove).toHaveBeenCalledTimes(2);
  expect(claim()).toBeNull();
});

it("stops release retries at its deadline and preserves recoverable ownership", async () => {
  const { store, db, claim } = fixture();
  const lock = persistentMigrationLock(store, { waitTimeoutMs: 0 });
  await lock.acquire(db);
  store.remove = vi.fn(async () => {
    throw { code: "40001" };
  });
  await expect(lock.release(db)).rejects.toMatchObject({ code: "ANTFLY_MIGRATION_LOCK_UNKNOWN" });
  expect(store.remove).toHaveBeenCalledTimes(1);
  expect(claim()).not.toBeNull();
});
