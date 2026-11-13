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

// Run through test_pgwire_drivers.py after building @antfly/sdk.
import { inspect } from "node:util";
import pg from "pg";
import { AntflyClient } from "../../ts/packages/sdk/dist/index.js";
import {
	AntflyPersistentMigrationLock,
	createAntflyPostgres,
} from "../../ts/packages/sdk/dist/kysely.js";

// Exercise parent-side failure reporting without creating a database claim.
if (process.argv[2] === "exit-before-report") process.exit(17);

// Keep bounded phase evidence in the subprocess outcome. In particular, a
// contender timeout must distinguish a slow owner DDL/release from stale reads.
// Parameters and connection URLs are never included in these diagnostics.
const started = performance.now();
const phases = new Map();
const slowest = [];
const record = (phase, before) => {
	const elapsed = performance.now() - before;
	const entry = phases.get(phase) ?? { calls: 0, milliseconds: 0 };
	entry.calls++;
	entry.milliseconds += elapsed;
	phases.set(phase, entry);
	slowest.push({ phase, start_ms: Math.round(before - started), duration_ms: Math.round(elapsed) });
	slowest.sort((a, b) => b.duration_ms - a.duration_ms);
	slowest.length = Math.min(slowest.length, 8);
};
const fetch = globalThis.fetch;
globalThis.fetch = async (...args) => {
	const before = performance.now();
	try { return await fetch(...args); }
	finally { record("HTTP", before); }
};
const wrapped = Symbol("migration query timings");
class TimedPool extends pg.Pool {
	async connect() {
		const connection = await super.connect();
		if (!connection[wrapped]) {
			connection[wrapped] = true;
			const original = connection.query.bind(connection);
			connection.query = async (...args) => {
				const sql = typeof args[0] === "string" ? args[0] : args[0]?.text ?? "";
				const verb = sql.match(/^\s*(\w+)/)?.[1]?.toUpperCase() ?? "UNKNOWN";
				const phase = sql.includes("_antfly_kysely_migration_claims") ? `CLAIM ${verb}` : verb;
				const before = performance.now();
				try {
					return await original(...args);
				} finally {
					record(phase, before);
				}
			};
		}
		return connection;
	}
}
const timings = () => ({
	total_ms: Math.round(performance.now() - started),
	queries: Object.fromEntries(phases),
	slowest,
});

const url = new URL(process.env.ANTFLY_PGWIRE_URL);
const client = new AntflyClient({
	baseUrl: process.env.ANTFLY_HTTP_URL,
	auth: {
		username: decodeURIComponent(url.username),
		password: decodeURIComponent(url.password),
	},
});
const runtime = createAntflyPostgres({
	pg: { ...pg, Pool: TimedPool },
	client,
	pool: { connectionString: url.toString() },
});
try {
	if (process.argv[2] === "claim") {
		await new AntflyPersistentMigrationLock().acquire(runtime.db);
		process.send({ claimed: await runtime.inspectMigrationLock() });
		// Parent deliberately terminates this process to exercise abandoned ownership.
		await new Promise(() => {});
	} else {
		const result = await runtime.migrateToLatest({
			migrationTableName: "process_migrations",
			migrationLockTableName: "process_lock",
			provider: {
				async getMigrations() {
					return {
						"001": {
							async up(db) {
								await db.schema
									.createTable("process_created")
									.addColumn("id", "text")
									.execute();
							},
						},
					};
				},
			},
		});
		process.send({
			error: result.error ? inspect(result.error, { depth: 5 }) : null,
			applied: result.results?.length ?? 0,
			timings: timings(),
		});
	}
} catch (error) {
	process.send({ error: inspect(error, { depth: 5 }), timings: timings() });
	process.exitCode = 1;
} finally {
	await runtime.close();
	process.disconnect();
}
