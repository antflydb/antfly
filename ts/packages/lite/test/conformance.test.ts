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
 * Runs the shared libantfly conformance cases (see
 * zig/pkg/antfly/capi-conformance/README.md) through the public @antfly/lite
 * API, mirroring go/pkg/lite/conformance_cgo_test.go's semantics exactly so
 * every binding stays behaviorally identical.
 */
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, it } from "vitest";
import { createWithOptions, type Database, openWithOptions } from "../src/database.js";
import { AntflyError } from "../src/errors.js";
import { restoreBackup } from "../src/files.js";
import { GraphDirection, OpenMode, Profile, TxnStatus } from "../src/types.js";
import { describeWithLibrary } from "./helpers.js";

const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const casesDir = join(
  packageDir,
  "..",
  "..",
  "..",
  "zig",
  "pkg",
  "antfly",
  "capi-conformance",
  "cases"
);

interface ConformanceOpen {
  create?: boolean;
  mode?: "writer" | "readonly" | "status_only";
  profile?: "native" | "hosted";
  no_sync?: boolean;
  busy_timeout_ms?: number;
  path?: string;
}

interface ConformanceWrite {
  key: string;
  value?: unknown;
  delete?: boolean;
}

interface ConformanceExpect {
  error?: string;
  json_subset?: unknown;
  contains?: string[];
  not_contains?: string[];
  equals?: unknown;
}

interface ConformanceStep extends ConformanceOpen {
  op: string;
  writes?: ConformanceWrite[];
  timestamp?: number;
  key?: string;
  request?: unknown;
  config?: unknown;
  schema?: unknown;
  name?: string;
  kind?: string;
  txn_id?: string;
  status?: "committed" | "aborted";
  commit_version?: number;
  index?: string;
  edge_type?: string;
  direction?: "out" | "in" | "both";
  expect?: ConformanceExpect;
}

interface ConformanceCase {
  name: string;
  description?: string;
  open: ConformanceOpen;
  steps: ConformanceStep[];
}

function conformanceOpenOptions(o: ConformanceOpen) {
  const opts: { mode: OpenMode; profile: Profile; noSync: boolean; busyTimeoutMs: number } = {
    mode: OpenMode.Writer,
    profile: Profile.Native,
    noSync: Boolean(o.no_sync),
    busyTimeoutMs: o.busy_timeout_ms ?? 0,
  };
  switch (o.mode ?? "writer") {
    case "writer":
      opts.mode = OpenMode.Writer;
      break;
    case "readonly":
      opts.mode = OpenMode.Readonly;
      break;
    case "status_only":
      opts.mode = OpenMode.StatusOnly;
      break;
    default:
      throw new Error(`unknown mode ${o.mode}`);
  }
  switch (o.profile ?? "native") {
    case "native":
      opts.profile = Profile.Native;
      break;
    case "hosted":
      opts.profile = Profile.Hosted;
      break;
    default:
      throw new Error(`unknown profile ${o.profile}`);
  }
  return opts;
}

function conformanceWrites(writes: ConformanceWrite[] | undefined) {
  return (writes ?? []).map((w) => ({
    key: w.key,
    delete: Boolean(w.delete),
    value: w.delete ? undefined : (w.value as Record<string, unknown> | undefined),
  }));
}

function directionFromString(direction: ConformanceStep["direction"]): GraphDirection {
  switch (direction ?? "out") {
    case "out":
      return GraphDirection.Out;
    case "in":
      return GraphDirection.In;
    case "both":
      return GraphDirection.Both;
    default:
      throw new Error(`unknown direction ${direction}`);
  }
}

function txnIdHex(id: string | undefined): string {
  if (!id || !/^[0-9a-fA-F]{32}$/.test(id)) {
    throw new Error(`txn_id ${JSON.stringify(id)} must be 32 hex characters`);
  }
  return id;
}

function jsonSubset(want: unknown, got: unknown): boolean {
  if (Array.isArray(want)) {
    if (!Array.isArray(got) || got.length !== want.length) return false;
    return want.every((w, i) => jsonSubset(w, got[i]));
  }
  if (want !== null && typeof want === "object") {
    if (got === null || typeof got !== "object" || Array.isArray(got)) return false;
    const gotObj = got as Record<string, unknown>;
    return Object.entries(want as Record<string, unknown>).every(
      ([k, wv]) => k in gotObj && jsonSubset(wv, gotObj[k])
    );
  }
  return Object.is(want, got);
}

function resultText(result: unknown): string {
  if (result === null || result === undefined) return "";
  if (Buffer.isBuffer(result)) return result.toString("utf8");
  if (typeof result === "string") return result;
  if (typeof result === "bigint") return result.toString();
  return JSON.stringify(result);
}

function checkConformanceResult(result: unknown, expect_: ConformanceExpect): void {
  const text = resultText(result);
  if (expect_.json_subset !== undefined) {
    const got = text.length > 0 ? JSON.parse(text) : null;
    if (!jsonSubset(expect_.json_subset, got)) {
      throw new Error(`result ${text} does not contain ${JSON.stringify(expect_.json_subset)}`);
    }
  }
  for (const s of expect_.contains ?? []) {
    if (!text.includes(s)) {
      throw new Error(`result ${text} does not contain ${JSON.stringify(s)}`);
    }
  }
  for (const s of expect_.not_contains ?? []) {
    if (text.includes(s)) {
      throw new Error(`result ${text} unexpectedly contains ${JSON.stringify(s)}`);
    }
  }
  if (expect_.equals !== undefined) {
    let got: unknown;
    if (typeof result === "boolean" || typeof result === "number" || typeof result === "string") {
      got = result;
    } else if (typeof result === "bigint") {
      got = Number(result);
    } else {
      got = text.length > 0 ? JSON.parse(text) : null;
    }
    if (!Object.is(got, expect_.equals) && got !== expect_.equals) {
      throw new Error(`result ${JSON.stringify(got)}, want ${JSON.stringify(expect_.equals)}`);
    }
  }
}

class ConformanceRunner {
  dir: string;
  db?: Database;
  path?: string;
  backup?: Uint8Array;

  constructor(dir: string) {
    this.dir = dir;
  }

  resolvePath(name?: string): string {
    return join(this.dir, name || "db.aflite");
  }

  async openCurrent(o: ConformanceOpen): Promise<void> {
    const opts = conformanceOpenOptions(o);
    const path = this.resolvePath(o.path);
    this.db = o.create ? await createWithOptions(path, opts) : await openWithOptions(path, opts);
    this.path = path;
  }

  async closeCurrent(): Promise<void> {
    if (this.db) {
      await this.db.close();
      this.db = undefined;
    }
  }

  private requireDb(): Database {
    if (!this.db) throw new Error("no open database handle");
    return this.db;
  }

  async runStep(step: ConformanceStep): Promise<void> {
    let result: unknown;
    let error: unknown;
    try {
      result = await this.execute(step);
    } catch (err) {
      error = err;
    }
    const expect_ = step.expect;
    if (!expect_) {
      if (error) throw error;
      return;
    }
    if (expect_.error) {
      if (!error) throw new Error(`succeeded, want ${expect_.error}`);
      if (!(error instanceof AntflyError)) {
        throw new Error(`error ${error} is not an AntflyError, want ${expect_.error}`);
      }
      if (error.codeName !== expect_.error) {
        throw new Error(`error ${error.codeName}, want ${expect_.error}`);
      }
      return;
    }
    if (error) throw error;
    checkConformanceResult(result, expect_);
  }

  private async execute(step: ConformanceStep): Promise<unknown> {
    const db = this.requireDb.bind(this);
    switch (step.op) {
      case "batch":
        return db().batch(conformanceWrites(step.writes), step.timestamp ?? 0);
      case "batch_json":
        return db().batchJsonRaw(step.request as never);
      case "lookup":
        return db().lookupRaw(step.key ?? "");
      case "scan":
        return db().scanRaw(step.request as never);
      case "search":
        return db().searchRaw(step.request as never);
      case "stats":
        return db().statsRaw();
      case "status":
        return db().statusRaw();
      case "capabilities":
        return db().capabilitiesRaw();
      case "check":
        return db().checkRaw();
      case "pending_work_stats":
        return db().pendingWorkStatsRaw();
      case "run_until_idle":
        return db().runUntilIdle();
      case "get_schema":
        return db().getSchemaRaw();
      case "set_schema":
        return db().setSchema(step.schema as never);
      case "list_indexes":
        return db().listIndexesRaw();
      case "add_index":
        return db().addIndex(step.config as never);
      case "delete_index":
        return db().deleteIndex(step.name ?? "");
      case "list_enrichments":
        return db().listEnrichmentsRaw();
      case "add_enrichment":
        return db().addEnrichment(step.config as never);
      case "delete_enrichment":
        return db().deleteEnrichment(step.kind ?? "", step.name ?? "");
      case "get_edges":
        return db().edgesRaw(
          step.index ?? "",
          step.key ?? "",
          step.edge_type ?? "",
          directionFromString(step.direction)
        );
      case "begin_transaction":
      case "write_transaction":
      case "resolve_transaction":
      case "transaction_status":
      case "commit_version":
        return this.executeTransaction(step);
      case "backup": {
        this.backup = await db().backup();
        return null;
      }
      case "restore_open": {
        const path = this.resolvePath(step.path);
        if (!this.backup) throw new Error("no backup captured yet");
        await restoreBackup(path, this.backup, false);
        await this.closeCurrent();
        await this.openCurrent(step);
        return null;
      }
      case "reopen": {
        await this.closeCurrent();
        const o = { ...step } as ConformanceOpen;
        if (!o.path && this.path) o.path = basename(this.path);
        await this.openCurrent(o);
        return null;
      }
      case "open_second": {
        const opts = conformanceOpenOptions(step);
        const path = this.resolvePath(step.path);
        const second = step.create
          ? await createWithOptions(path, opts)
          : await openWithOptions(path, opts);
        await second.close();
        return null;
      }
      case "close":
        await this.closeCurrent();
        return null;
      default:
        throw new Error(`unknown op ${step.op}`);
    }
  }

  private async executeTransaction(step: ConformanceStep): Promise<unknown> {
    const id = txnIdHex(step.txn_id);
    const db = this.requireDb();
    switch (step.op) {
      case "begin_transaction":
        return db.beginTransaction(id, step.timestamp ?? 0);
      case "write_transaction":
        return db.writeTransaction(id, conformanceWrites(step.writes));
      case "resolve_transaction": {
        const status = step.status === "aborted" ? TxnStatus.Aborted : TxnStatus.Committed;
        if (step.status !== "aborted" && step.status !== "committed") {
          throw new Error(`unknown status ${step.status}`);
        }
        return db.resolveTransaction(id, status, step.commit_version ?? 0);
      }
      case "transaction_status": {
        const status = await db.transactionStatus(id);
        return {
          [TxnStatus.Pending]: "pending",
          [TxnStatus.Committed]: "committed",
          [TxnStatus.Aborted]: "aborted",
        }[status];
      }
      default:
        return db.commitVersion(id);
    }
  }
}

function loadCases(): ConformanceCase[] {
  if (!existsSync(casesDir)) return [];
  return readdirSync(casesDir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(readFileSync(join(casesDir, f), "utf8")) as ConformanceCase);
}

describeWithLibrary("conformance", () => {
  const cases = loadCases();
  if (cases.length === 0) {
    it("no conformance cases found", () => {
      expect.fail(`no conformance cases in ${casesDir}`);
    });
    return;
  }

  const rootTmp = mkdtempSync(join(tmpdir(), "antfly-lite-conformance-"));

  for (const testCase of cases) {
    it(testCase.name, async () => {
      const dir = join(rootTmp, testCase.name);
      mkdirSync(dir, { recursive: true });
      const runner = new ConformanceRunner(dir);
      try {
        await runner.openCurrent(testCase.open);
        for (let i = 0; i < testCase.steps.length; i++) {
          const step = testCase.steps[i];
          if (!step) continue;
          try {
            await runner.runStep(step);
          } catch (err) {
            throw new Error(`step ${i} (${step.op}): ${(err as Error).message}`, { cause: err });
          }
        }
      } finally {
        await runner.closeCurrent();
        rmSync(dir, { recursive: true, force: true });
      }
    });
  }
});
