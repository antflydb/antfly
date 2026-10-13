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

const form = document.querySelector('#search');
const status = document.querySelector('#status');
const results = document.querySelector('#results');
const more = document.querySelector('#more');
let search, cursor, controller;

async function load(next = false) {
  controller?.abort();
  controller = new AbortController();
  const active = controller;
  if (!next) {
    search = { q: document.querySelector('#q').value, view: document.querySelector('#view').value };
    cursor = null;
    results.replaceChildren();
  }
  more.hidden = true;
  status.textContent = 'Searching…';
  try {
    const response = await fetch('/api/search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...search, ...(cursor ? { cursor } : {}) }), signal: active.signal });
    const data = await response.json();
    if (active !== controller) return;
    if (!response.ok) throw new Error(data.error || 'Search failed');
    if (data.recent_since !== undefined) search.recent_since = data.recent_since;
    for (const hit of data.hits.hits) {
      const row = hit._source;
      const item = document.createElement('li');
      const link = document.createElement('a');
      const id = String(row.hn_id);
      if (!/^\d+$/.test(id)) continue;
      link.href = `https://news.ycombinator.com/item?id=${id}`;
      link.textContent = row.title || `Comment by ${row.author || 'unknown'}`;
      const meta = document.createElement('p');
      const date = new Date(Number(row.created_at) * 1000);
      meta.textContent = `${row.author || 'unknown'} · ${row.points ?? 0} points${Number.isNaN(date.getTime()) ? '' : ' · ' + date.toLocaleDateString()}`;
      const snippet = document.createElement('p');
      // HN content and highlights are untrusted; never interpret them as HTML.
      const highlight = hit._highlights?.body;
      snippet.textContent = String(Array.isArray(highlight) ? highlight.join(' … ') : highlight || row.body || '').slice(0, 600).replace(/<[^>]*>/g, '');
      item.append(link, meta, snippet);
      results.append(item);
    }
    cursor = data.cursor;
    more.hidden = !cursor;
    status.textContent = results.children.length ? `${results.children.length} results shown` : 'No results';
  } catch (error) {
    if (error.name !== 'AbortError') status.textContent = error.message;
  }
}
form.addEventListener('submit', event => { event.preventDefault(); load(); });
more.addEventListener('click', () => load(true));
