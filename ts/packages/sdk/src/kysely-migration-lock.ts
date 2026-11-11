// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import { type Kysely, sql } from "kysely";
import { MIGRATION_CLAIMS_TABLE } from "./kysely-internals.js";
import type { AntflyMigrationLock } from "./kysely-postgres-dialect.js";

export interface AntflyMigrationClaim {
  ownerToken: string;
  owner: string;
  acquiredAt: string;
}

export interface AntflyMigrationLockSettings {
  /** All runners in a database must use the same claim namespace. Defaults to public. */
  namespace?: string;
  /** Acquisition deadline only; claims never expire or transfer automatically. */
  waitTimeoutMs?: number;
  owner?: string;
}

export class AntflyMigrationLockError extends Error {
  constructor(
    readonly code:
      | "ANTFLY_MIGRATION_LOCK_BUSY"
      | "ANTFLY_MIGRATION_LOCK_UNKNOWN"
      | "ANTFLY_MIGRATION_LOCK_LOST",
    message: string,
    readonly claim: AntflyMigrationClaim | null,
    readonly cause?: unknown
  ) {
    super(message);
    this.name = "AntflyMigrationLockError";
  }
}

const CLAIM_KEY = "catalog-migrations";
const validToken = (value: string) =>
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
const state = (error: unknown): unknown =>
  error && typeof error === "object"
    ? "code" in error
      ? error.code
      : "sqlstate" in error
        ? error.sqlstate
        : undefined
    : undefined;

/** Internal storage seam for fault-injection tests; not exported by the SDK entry point. */
export interface MigrationClaimStore {
  ensure(db: Kysely<unknown>): Promise<void>;
  read(db: Kysely<unknown>): Promise<AntflyMigrationClaim | null>;
  insert(db: Kysely<unknown>, claim: AntflyMigrationClaim): Promise<void>;
  remove(db: Kysely<unknown>, ownerToken: string): Promise<boolean>;
}

function claimStore(namespace: string): MigrationClaimStore {
  const tableName = `${namespace}.${MIGRATION_CLAIMS_TABLE}`;
  return {
    async ensure(db) {
      try {
        await sql`SELECT owner_token, owner_label, acquired_at FROM ${sql.table(tableName)} LIMIT 0`.execute(
          db
        );
        return;
      } catch (error) {
        if (state(error) !== "42P01") throw error;
      }
      // _id is the engine's unique physical row key. No SQL secondary-index
      // assumption or client-side check-then-insert is used to claim ownership.
      await db.schema
        .withSchema(namespace)
        .createTable(MIGRATION_CLAIMS_TABLE)
        .ifNotExists()
        .addColumn("owner_token", "text", (column) => column.notNull())
        .addColumn("owner_label", "text", (column) => column.notNull())
        .addColumn("acquired_at", "text", (column) => column.notNull())
        .execute();
      await sql`SELECT owner_token, owner_label, acquired_at FROM ${sql.table(tableName)} LIMIT 0`.execute(
        db
      );
    },
    async read(db) {
      let rows: AntflyMigrationClaim[];
      try {
        ({ rows } =
          await sql<AntflyMigrationClaim>`SELECT owner_token AS "ownerToken", owner_label AS "owner", acquired_at AS "acquiredAt"
        FROM ${sql.table(tableName)} WHERE _id = ${CLAIM_KEY}`.execute(db));
      } catch (error) {
        if (state(error) === "42P01") return null;
        throw error;
      }
      if (!rows.length) return null;
      const claim = rows[0];
      if (
        rows.length !== 1 ||
        !claim ||
        typeof claim.ownerToken !== "string" ||
        !validToken(claim.ownerToken) ||
        typeof claim.owner !== "string" ||
        typeof claim.acquiredAt !== "string"
      ) {
        throw new Error("Invalid Antfly migration claim metadata");
      }
      return claim;
    },
    async insert(db, claim) {
      await sql`INSERT INTO ${sql.table(tableName)} (_id, owner_token, owner_label, acquired_at)
      VALUES (${CLAIM_KEY}, ${claim.ownerToken}, ${claim.owner}, ${claim.acquiredAt})`.execute(db);
    },
    async remove(db, ownerToken) {
      const result =
        await sql`DELETE FROM ${sql.table(tableName)} WHERE _id = ${CLAIM_KEY} AND owner_token = ${ownerToken}`.execute(
          db
        );
      return result.numAffectedRows === 1n;
    },
  };
}

/** Internal protocol implementation, allowing storage failures to be exercised independently. */
export function persistentMigrationLock(
  storage: MigrationClaimStore,
  options: AntflyMigrationLockSettings = {}
) {
  const wait = options.waitTimeoutMs ?? 30_000;
  if (!Number.isSafeInteger(wait) || wait < 0 || wait > 86_400_000)
    throw new RangeError("waitTimeoutMs must be between 0 and 86400000");
  const owner = options.owner ?? "kysely";
  if (!owner || owner.length > 256)
    throw new TypeError("Migration owner must contain 1 to 256 characters");
  const held = new WeakMap<Kysely<unknown>, AntflyMigrationClaim>();
  return {
    async acquire(db: Kysely<unknown>) {
      if (held.has(db)) throw new Error("Migration lock already acquired on this connection");
      const claim = {
        ownerToken: crypto.randomUUID(),
        owner,
        acquiredAt: new Date().toISOString(),
      };
      const deadline = performance.now() + wait;
      for (;;) {
        try {
          await storage.ensure(db);
          break;
        } catch (error) {
          // These SQLSTATEs guarantee rejection, not an uncertain DDL commit.
          if (!["40001", "53300"].includes(String(state(error))) || performance.now() >= deadline)
            throw error;
          await new Promise((resolve) =>
            setTimeout(resolve, Math.min(100, Math.max(1, deadline - performance.now())))
          );
        }
      }
      for (;;) {
        let rejected = false;
        let writeError: unknown;
        try {
          await storage.insert(db, claim);
        } catch (error) {
          // Unique conflicts and serialization failures guarantee no commit.
          // Reconcile the token before retrying this internal claim statement.
          rejected = ["23505", "40001", "53300"].includes(String(state(error)));
          writeError = error;
        }
        let observed: AntflyMigrationClaim | null;
        try {
          for (;;) {
            try {
              observed = await storage.read(db);
              break;
            } catch (readError) {
              // Repeating a read cannot replay a migration or claim write.
              if (
                !["40001", "53300"].includes(String(state(readError))) ||
                performance.now() >= deadline
              )
                throw readError;
              await new Promise((resolve) =>
                setTimeout(resolve, Math.min(100, Math.max(1, deadline - performance.now())))
              );
            }
          }
        } catch (error) {
          throw new AntflyMigrationLockError(
            "ANTFLY_MIGRATION_LOCK_UNKNOWN",
            "Migration claim ownership could not be confirmed; no migration was started",
            claim,
            { writeError, readError: error }
          );
        }
        if (observed?.ownerToken === claim.ownerToken) {
          held.set(db, claim);
          return;
        }
        if (!rejected) {
          throw new AntflyMigrationLockError(
            "ANTFLY_MIGRATION_LOCK_UNKNOWN",
            "Migration claim was not confirmed; reconcile this owner token before recovery",
            claim,
            writeError
          );
        }
        if (performance.now() >= deadline) {
          throw new AntflyMigrationLockError(
            "ANTFLY_MIGRATION_LOCK_BUSY",
            "Migration lock is occupied; claims do not expire automatically",
            observed
          );
        }
        await new Promise((resolve) =>
          setTimeout(resolve, Math.min(100, Math.max(1, deadline - performance.now())))
        );
      }
    },
    async release(db: Kysely<unknown>) {
      const claim = held.get(db);
      // Kysely calls release even when acquire fails. Never remove an
      // unconfirmed claim, including one whose INSERT might still commit.
      if (!claim) return;
      const deadline = performance.now() + wait;
      for (;;) {
        try {
          if (!(await storage.remove(db, claim.ownerToken))) {
            held.delete(db);
            throw new AntflyMigrationLockError(
              "ANTFLY_MIGRATION_LOCK_LOST",
              "Migration claim ownership changed before release",
              claim
            );
          }
          held.delete(db);
          return;
        } catch (error) {
          if (error instanceof AntflyMigrationLockError) throw error;
          // These SQLSTATEs guarantee the conditional DELETE did not commit.
          // Retain the same owner token and retry only this internal release;
          // uncertain writes and application migrations are never replayed.
          if (["40001", "53300"].includes(String(state(error))) && performance.now() < deadline) {
            await new Promise((resolve) =>
              setTimeout(resolve, Math.min(100, Math.max(1, deadline - performance.now())))
            );
            continue;
          }
          throw new AntflyMigrationLockError(
            "ANTFLY_MIGRATION_LOCK_UNKNOWN",
            "Migration claim release was not confirmed; do not replay migrations",
            claim,
            error
          );
        }
      }
    },
  };
}

/** Cross-process, persistent, non-expiring ownership for one catalog's migrations. */
export class AntflyPersistentMigrationLock implements AntflyMigrationLock {
  private readonly protocol;
  private readonly store;
  constructor(options: AntflyMigrationLockSettings = {}) {
    const namespace = options.namespace ?? "public";
    if (!/^[_a-zA-Z][_a-zA-Z0-9-]{0,127}$/.test(namespace))
      throw new TypeError("Invalid migration claim namespace");
    this.store = claimStore(namespace);
    this.protocol = persistentMigrationLock(this.store, options);
  }
  acquire(db: Kysely<unknown>): Promise<void> {
    return this.protocol.acquire(db);
  }
  release(db: Kysely<unknown>): Promise<void> {
    return this.protocol.release(db);
  }
  inspect(db: Kysely<unknown>): Promise<AntflyMigrationClaim | null> {
    return this.store.read(db);
  }
  async recover(
    db: Kysely<unknown>,
    options: { ownerToken: string; previousRunnerStopped: true }
  ): Promise<boolean> {
    if (options.previousRunnerStopped !== true || !validToken(options.ownerToken))
      throw new TypeError(
        "Recovery requires the observed owner token and confirmation that the previous runner stopped"
      );
    try {
      return await this.store.remove(db, options.ownerToken);
    } catch (error) {
      if (state(error) === "42P01") return false;
      throw error;
    }
  }
}
