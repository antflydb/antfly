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

import app, { CommitCoordinator as Coordinator } from "../worker.mjs";
// This wrapper is bundled only by the disposable qualification configuration.
export class CommitCoordinator extends Coordinator {
  async destroyQualificationState() { await this.ctx.storage.deleteAll(); }
}
export default {
  ...app,
  async fetch(request, env, ctx) {
    if (new URL(request.url).pathname !== "/_qualification/drain") return app.fetch(request, env, ctx);
    if (request.method !== "POST" || !env.ANTFLY_INCARNATION.startsWith("qualification-") || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`) return new Response("unauthorized", {status: 401});
    const page = await env.DATABASE.list({limit: 128});
    if (page.objects.length) await env.DATABASE.delete(page.objects.map(item => item.key));
    if (!page.truncated && !page.objects.length) await env.COMMITS.getByName(env.ANTFLY_INCARNATION).destroyQualificationState();
    return Response.json({done: !page.objects.length, deleted: page.objects.length});
  },
};
