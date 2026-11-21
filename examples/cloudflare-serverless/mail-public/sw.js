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

// Offline shell for Antfly Mail. Caches only public application code:
// - the shell is network-first so deployments take effect on the next load;
// - versioned runtime assets under /local/ are cache-first (immutable paths);
// - the runtime config and model catalog are network-first, so on-device
//   search can reopen offline;
// - mail, auth and model weights always go to the network and are never
//   stored here. Message data lives only in the opt-in OPFS copy, and the
//   weights in @antfly/web's verified OPFS model cache.
const SHELL = "antfly-mail-shell-v1";
const RUNTIME = "antfly-mail-runtime-v1";
const SHELL_FILES = [
  "/",
  "/mail.css",
  "/mail.js",
  "/local-index.js",
  "/local-schema.js",
  "/local-db-worker.js",
  "/manifest.webmanifest",
  "/icon.svg",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches
      .open(SHELL)
      .then((cache) => cache.addAll(SHELL_FILES))
      .then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches
      .keys()
      .then((keys) => Promise.all(keys.filter((k) => ![SHELL, RUNTIME].includes(k)).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

// Small descriptors needed to reopen the on-device index offline. Network-first.
const DESCRIPTORS = ["/local/config.json", "/local/models/catalog.json"];
function privatePath(pathname) {
  return (
    pathname.startsWith("/mail/") ||
    pathname.startsWith("/api/") ||
    (pathname.startsWith("/local/models/") && !DESCRIPTORS.includes(pathname))
  );
}

self.addEventListener("fetch", (event) => {
  const request = event.request;
  const url = new URL(request.url);
  if (request.method !== "GET" || url.origin !== self.location.origin || privatePath(url.pathname)) return;

  if (url.pathname.startsWith("/local/") && !DESCRIPTORS.includes(url.pathname)) {
    // Runtime directories are versioned by content; a hit never goes stale.
    event.respondWith(
      caches.open(RUNTIME).then(async (cache) => {
        const hit = await cache.match(request);
        if (hit) return hit;
        const response = await fetch(request);
        if (response.ok && response.type === "basic") await cache.put(request, response.clone());
        return response;
      })
    );
    return;
  }

  event.respondWith(
    (async () => {
      const cache = await caches.open(SHELL);
      try {
        const response = await fetch(request);
        if (response.ok && response.type === "basic") {
          const key = request.mode === "navigate" ? "/" : request;
          await cache.put(key, response.clone());
        }
        return response;
      } catch (error) {
        const hit = await cache.match(request.mode === "navigate" ? "/" : request);
        if (hit) return hit;
        throw error;
      }
    })()
  );
});
