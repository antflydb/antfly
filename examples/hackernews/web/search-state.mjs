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

const options = {
  view: ['all', 'recent', 'history'], type: ['all', 'story', 'comment'],
  sort: ['relevance', 'date'], range: ['all', 'day', 'week', 'month', 'year', 'custom'],
};
export const defaults = { q: '', view: 'all', type: 'all', sort: 'relevance', range: 'all', from: '', to: '' };

function dateEpoch(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw new Error('Choose a valid date.');
  const timestamp = Date.parse(value + 'T00:00:00Z');
  if (!Number.isFinite(timestamp) || timestamp < 0 || new Date(timestamp).toISOString().slice(0, 10) !== value) {
    throw new Error('Choose a valid date from 1970 onward.');
  }
  return timestamp / 1000;
}

export function fromURL(query) {
  const params = new URLSearchParams(query);
  const state = { ...defaults, q: (params.get('q') || '').slice(0, 256) };
  for (const [key, values] of Object.entries(options)) {
    if (values.includes(params.get(key))) state[key] = params.get(key);
  }
  for (const key of ['from', 'to']) {
    const value = params.get(key) || '';
    if (/^\d{4}-\d{2}-\d{2}$/.test(value)) state[key] = value;
  }
  return state;
}

export function toURL(state) {
  const params = new URLSearchParams();
  if (state.q.trim()) params.set('q', state.q.trim());
  for (const key of Object.keys(options)) {
    if (!options[key].includes(state[key])) throw new Error('Invalid search option.');
    if (state[key] !== defaults[key]) params.set(key, state[key]);
  }
  if (state.range === 'custom') {
    if (state.from) params.set('from', state.from);
    if (state.to) params.set('to', state.to);
  }
  return params.toString();
}

export function requestFor(state, now = Date.now()) {
  toURL(state);
  const request = { q: state.q.trim(), view: state.view, type: state.type, sort: state.sort };
  const days = { day: 1, week: 7, month: 30, year: 365 };
  if (days[state.range]) request.date_from = Math.max(0, Math.floor(now / 1000) - days[state.range] * 86400);
  if (state.range === 'custom') {
    if (!state.from && !state.to) throw new Error('Choose a start or end date.');
    if (state.from) request.date_from = dateEpoch(state.from);
    if (state.to) request.date_to = dateEpoch(state.to) + 86400;
    if (request.date_from !== undefined && request.date_to !== undefined && request.date_from >= request.date_to) {
      throw new Error('The start date must be on or before the end date.');
    }
  }
  return request;
}
