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

import app, { CommitCoordinator as Coordinator, R2CommitCoordinator as R2Coordinator, R2ReadRuntime as ReadRuntime, R2BuildRuntime as BuildRuntime } from "../worker.mjs";
export {R2ReadRuntime,R2BuildRuntime} from "../worker.mjs";
// This wrapper is bundled only by the disposable qualification configuration.
export class CommitCoordinator extends Coordinator {
  collection(op, args) {
    try { return super.collection(op, args); }
    catch (error) { throw new Error(`Collection ${op}: ${error.message}`); }
  }
  collectionDiagnostics() {
    const totals = this.ctx.storage.sql.exec("SELECT COUNT(*) AS tasks, SUM(done = 0) AS pending, MAX(length(task)) AS max_task_bytes FROM collection_tasks").toArray()[0];
    const marks = this.ctx.storage.sql.exec("SELECT COUNT(*) AS marks FROM collection_marks").toArray()[0];
    const next = this.ctx.storage.sql.exec("SELECT task FROM collection_tasks WHERE done = 0 ORDER BY id LIMIT 1").toArray()[0];
    return {...totals, ...marks, phase: this.journal.state()?.phase, next_kind: next ? JSON.parse(next.task).kind : null};
  }
  async destroyQualificationState() { await this.ctx.storage.deleteAll(); }
}
export class R2CommitCoordinator extends R2Coordinator {
  collectionDiagnostics() {
    return this.store.transaction(async tx => ({phase: (await tx.get("__antfly_collection"))?.phase, tasks: (await tx.list({prefix:"__antfly_gc/tasks/"})).size, pending: (await tx.list({prefix:"__antfly_gc/pending/"})).size, marks: (await tx.list({prefix:"__antfly_gc/marks/"})).size, bounded: true}));
  }
  async destroyQualificationState() { /* Durable state was drained from R2. */ }
}
export class ReopenedR2CommitCoordinator extends R2CommitCoordinator {}
export class ColdR2CommitCoordinator extends R2CommitCoordinator {}
export class ColdReadRuntime extends ReadRuntime {}
export class ColdBuildRuntime extends BuildRuntime {}
export class FinalR2CommitCoordinator extends R2CommitCoordinator {}
export class FinalReadRuntime extends ReadRuntime {}
export class FinalBuildRuntime extends BuildRuntime {}
export class ReopenedReadRuntime extends ReadRuntime {}
export class ReopenedBuildRuntime extends BuildRuntime {}
export default {
  ...app,
  async fetch(request, env, ctx) {
    const path = new URL(request.url).pathname;
    if (!["/_qualification/drain", "/_qualification/diagnostics", "/_qualification/version"].includes(path)) return app.fetch(request, env, ctx);
    if (request.method !== "POST" || !env.ANTFLY_INCARNATION.startsWith("qualification-") || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`) return new Response("unauthorized", {status: 401});
    if (path === "/_qualification/version") return Response.json({version:env.WORKER_VERSION?.id});
    if (path === "/_qualification/diagnostics") return Response.json(await env.COMMITS.getByName(env.ANTFLY_INCARNATION).collectionDiagnostics());
    const page = await env.DATABASE.list({limit: 128});
    if (page.objects.length) await env.DATABASE.delete(page.objects.map(item => item.key));
    if (!page.truncated && !page.objects.length && env.COORDINATOR_BACKEND !== "r2") await env.COMMITS.getByName(env.ANTFLY_INCARNATION).destroyQualificationState();
    return Response.json({done: !page.objects.length, deleted: page.objects.length, bytes: page.objects.reduce((sum,object)=>sum+object.size,0)});
  },
};
