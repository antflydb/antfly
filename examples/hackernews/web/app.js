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

import { defaults, fromURL, toURL, requestFor } from '/search-state.mjs';

const form = document.querySelector('#search');
const status = document.querySelector('#status');
const results = document.querySelector('#results');
const more = document.querySelector('#more');
const share = document.querySelector('#share');
const dates = document.querySelector('#custom-dates');
const controls = Object.fromEntries(Object.keys(defaults).map(key => [key, document.getElementById(key)]));
let search, cursor, controller;

function readControls() {
  return Object.fromEntries(Object.entries(controls).map(([key, input]) => [key, input.value]));
}

function restoreControls() {
  const state = fromURL(location.search);
  for (const [key, value] of Object.entries(state)) controls[key].value = value;
  dates.hidden = state.range !== 'custom';
  return state;
}

function renderHit(hit) {
  const row = hit._source;
  if (!row) return;
  const id = String(row.hn_id);
  if (!/^\d+$/.test(id)) return;
  const item = document.createElement('li');
  const kind = document.createElement('span');
  kind.className = 'item-kind';
  kind.textContent = row.item_type === 'comment' ? 'Comment' : row.item_type === 'story' ? 'Story' : String(row.item_type || 'Item');
  const link = document.createElement('a');
  link.href = 'https://news.ycombinator.com/item?id=' + id;
  link.textContent = row.title || (row.item_type === 'comment' ? 'Comment by ' + (row.author || 'unknown') : 'Untitled ' + (row.item_type || 'item'));
  const meta = document.createElement('p');
  meta.className = 'meta';
  const date = new Date(Number(row.created_at) * 1000);
  meta.textContent = (row.points ?? 0) + ' points · ' + (row.author || 'unknown')
    + (Number.isNaN(date.getTime()) ? '' : ' · ' + date.toLocaleString());
  const snippet = document.createElement('p');
  // HN content and highlights are untrusted; never interpret them as HTML.
  const highlight = hit._highlights?.body;
  snippet.textContent = String(Array.isArray(highlight) ? highlight.join(' … ') : highlight || row.body || '')
    .slice(0, 600).replace(/<[^>]*>/g, '');
  item.append(kind, link, meta, snippet);
  results.append(item);
}

async function load(next = false, updateHistory = true, restoredState = null) {
  controller?.abort();
  controller = new AbortController();
  const active = controller;
  if (!next) {
    cursor = null;
    search = null;
    results.replaceChildren();
  }
  more.hidden = true;
  results.setAttribute('aria-busy', 'true');
  try {
    if (!next) {
      // Date inputs sanitize invalid values; validate the original shared URL first.
      const state = restoredState || readControls();
      dates.hidden = state.range !== 'custom';
      const query = toURL(state);
      if (updateHistory && query !== location.search.slice(1)) {
        history.pushState(null, '', location.pathname + (query ? '?' + query : ''));
      }
      share.hidden = !state.q.trim();
      document.title = (state.q.trim() ? state.q.trim() + ' · ' : '') + 'Hacker News Search · Antfly';
      if (!state.q.trim()) {
        status.textContent = 'Search stories and comments from Hacker News.';
        return;
      }
      search = requestFor(state);
    }
    status.textContent = next ? 'Loading more results…' : 'Searching…';
    const response = await fetch('/api/search', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...search, ...(next && cursor ? { cursor } : {}) }),
      signal: active.signal,
    });
    const data = await response.json();
    if (active !== controller) return;
    if (!response.ok) throw new Error(data.error || 'Search failed. Please try again.');
    if (data.recent_since !== undefined) search.recent_since = data.recent_since;
    for (const hit of data.hits.hits) renderHit(hit);
    cursor = data.cursor;
    more.hidden = !cursor;
    status.textContent = results.children.length ? results.children.length + ' results shown' : 'No results. Try different search terms or filters.';
  } catch (error) {
    if (active !== controller || error.name === 'AbortError') return;
    status.textContent = error.message;
    if (!search) share.hidden = true;
    if (next && cursor) more.hidden = false;
  } finally {
    if (active === controller) results.setAttribute('aria-busy', 'false');
  }
}

form.addEventListener('submit', event => { event.preventDefault(); load(); });
form.addEventListener('change', event => {
  if (event.target === controls.q) return;
  dates.hidden = controls.range.value !== 'custom';
  load();
});
more.addEventListener('click', () => load(true));
share.addEventListener('click', async () => {
  const query = toURL(readControls());
  const link = location.origin + location.pathname + (query ? '?' + query : '');
  try {
    await navigator.clipboard.writeText(link);
    status.textContent = 'Search link copied.';
  } catch {
    status.textContent = 'Copy this search link: ' + link;
  }
});
window.addEventListener('popstate', () => load(false, false, restoreControls()));
load(false, false, restoreControls());
