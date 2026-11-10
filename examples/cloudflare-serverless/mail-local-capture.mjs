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

import { WorkerEntrypoint } from "cloudflare:workers";
/** Used only by the generated loopback dev configuration; no SMTP binding. */
export default class LocalMailCapture extends WorkerEntrypoint {
  async send(message) {
    if (this.env.AUTH_DEVELOPMENT !== "true") throw Error("LocalCaptureOnly");
    const id = crypto.randomUUID(),
      value = JSON.stringify(message);
    if (new TextEncoder().encode(value).length > 1024 * 1024)
      throw Error("LocalCaptureBudgetExceeded");
    await this.env.CAPTURE.put("mail/" + id, value, { onlyIf: { etagDoesNotMatch: "*" } });
    return { messageId: "local-" + id };
  }
  fetch() {
    return new Response("Local email capture has no public HTTP API.", { status: 404 });
  }
}
