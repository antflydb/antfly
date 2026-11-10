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

const url = new URL(process.env.ANTFLY_PGWIRE_URL);
const client = new AntflyClient({
	baseUrl: process.env.ANTFLY_HTTP_URL,
	auth: {
		username: decodeURIComponent(url.username),
		password: decodeURIComponent(url.password),
	},
});
const runtime = createAntflyPostgres({
	pg,
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
		});
	}
} catch (error) {
	process.send({ error: inspect(error, { depth: 5 }) });
	process.exitCode = 1;
} finally {
	await runtime.close();
	process.disconnect();
}
