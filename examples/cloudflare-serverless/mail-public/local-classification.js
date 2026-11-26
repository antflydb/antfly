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

export const categories = ["Primary", "Promotions", "Social", "Updates", "Forums"];

export const classificationLock = "antfly-mail-classification";

const empty = () => ({ cursor: "", labels: {} });

export class LocalClassification {
  constructor(scope) {
    this.scope = scope;
    this.state = empty();
  }

  static async open(scope) {
    const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(scope));
    return new LocalClassification(Array.from(new Uint8Array(digest), byte => byte.toString(16).padStart(2, "0")).join(""));
  }

  async directory(create = false) {
    const root = await navigator.storage.getDirectory();
    const mail = await root.getDirectoryHandle("antfly-mail-local", { create });
    return mail.getDirectoryHandle("classification", { create });
  }

  async read() {
    try {
      const file = await (await this.directory()).getFileHandle(this.scope + ".json");
      this.state = JSON.parse(await (await file.getFile()).text());
    } catch (error) {
      if (error.name !== "NotFoundError") throw error;
      this.state = empty();
    }
    return this.state;
  }

  async write(state) {
    const file = await (await this.directory(true)).getFileHandle(this.scope + ".json", { create: true });
    const writer = await file.createWritable();
    try {
      await writer.write(JSON.stringify(state));
      await writer.close();
    } catch (error) {
      await writer.abort().catch(() => {});
      throw error;
    }
    this.state = state;
  }

  lock(action, signal) {
    return navigator.locks.request(classificationLock, { signal }, action);
  }

  clear() {
    return this.lock(async () => {
      try {
        await (await this.directory()).removeEntry(this.scope + ".json");
      } catch (error) {
        if (error.name !== "NotFoundError") throw error;
      }
      this.state = empty();
    });
  }

  sync({ page, classifier, signal, onChange }) {
    return this.lock(async () => {
      let model;
      try {
        const previous = this.state.cursor;
        await this.read();
        if (previous !== this.state.cursor) onChange();
        let more = true;
        while (more) {
          signal.throwIfAborted();
          const update = await page(this.state.cursor, signal);
          const labels = update.reset ? {} : { ...this.state.labels };
          for (const id of update.deleted ?? []) delete labels[id];
          for (const message of update.messages) {
            signal.throwIfAborted();
            if (!message.text?.trim() && !message.subject?.trim()) {
              delete labels[message.id];
              continue;
            }
            model ??= await classifier();
            signal.throwIfAborted();
            labels[message.id] = await model.classify(message);
          }
          signal.throwIfAborted();
          if (update.reset || update.cursor !== this.state.cursor || update.messages.length || update.deleted?.length) {
            await this.write({ cursor: update.reset ? "" : update.cursor, labels });
            if (update.reset || update.messages.length || update.deleted?.length) onChange();
          }
          more = update.reset || update.more;
        }
      } finally {
        await model?.close();
      }
    }, signal);
  }
}
