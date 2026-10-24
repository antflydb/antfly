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

export class Bucket {
  objects = new Map(); revision = 0; writes = 0; readBytes = 0; reads = 0;
  metadata(key, object) { return { key, size: object.body.length, etag: object.etag, uploaded: new Date(0), httpMetadata: object.httpMetadata ?? {} }; }
  async put(key, body, options = {}) {
    await Promise.resolve();
    const old = this.objects.get(key), condition = options.onlyIf;
    if ((condition?.etagDoesNotMatch === "*" && old) || (condition?.etagMatches != null && old?.etag !== condition.etagMatches)) return null;
    const value = { body: body.slice(), etag: `e${++this.revision}`, httpMetadata: options.httpMetadata };
    this.objects.set(key, value); this.writes++;
    return this.metadata(key, value);
  }
  async get(key, options = {}) {
    await Promise.resolve(); this.reads++;
    const value = this.objects.get(key);
    if (!value) return null;
    const meta = this.metadata(key, value);
    if (options.onlyIf?.etagMatches != null && value.etag !== options.onlyIf.etagMatches) return meta;
    const offset = options.range?.offset ?? 0;
    const data = value.body.slice(offset, options.range?.length == null ? undefined : offset + options.range.length);
    this.readBytes += data.length;
    return { ...meta, range: options.range ? { offset, length: data.length } : undefined,
      body: new ReadableStream({ start(controller) { controller.enqueue(data); controller.close(); } }) };
  }
  async head(key) { const value = this.objects.get(key); return value ? this.metadata(key, value) : null; }
  async delete(key) { for (const item of Array.isArray(key) ? key : [key]) this.objects.delete(item); }
  async list(options = {}) {
    const keys = [...this.objects.keys()].sort().filter(key => key.startsWith(options.prefix ?? "") && (!options.startAfter || key > options.startAfter));
    const remaining = keys.filter(key => !options.cursor || key > options.cursor);
    const selected = remaining.slice(0, options.limit ?? 1000);
    const truncated = selected.length < remaining.length;
    return { objects: selected.map(key => this.metadata(key, this.objects.get(key))), truncated, cursor: truncated ? selected.at(-1) : undefined };
  }
}
