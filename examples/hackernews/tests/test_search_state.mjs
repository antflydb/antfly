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

import assert from 'node:assert/strict';
import test from 'node:test';
import { defaults, fromURL, toURL, requestFor } from '../web/search-state.mjs';

test('a shared URL restores every filter without carrying cursors or private options', () => {
  const state = { ...defaults, q: 'SQLite & vectors', type: 'comment', view: 'history',
    sort: 'date', range: 'custom', from: '2026-01-01', to: '2026-01-31' };
  assert.deepEqual(fromURL(toURL(state)), state);
  assert.deepEqual(requestFor(fromURL(toURL(state))), { q: state.q, type: 'comment',
    view: 'history', sort: 'date', date_from: Date.UTC(2026, 0, 1) / 1000,
    date_to: Date.UTC(2026, 1, 1) / 1000 });
  assert.equal(fromURL('?q=hello&type=arbitrary&sort=_id&view=secret&cursor=private').type, 'all');
  assert.equal(toURL({ ...defaults, q: 'database' }), 'q=database');
});

test('relative windows resolve once for a request and custom end dates include the full UTC day', () => {
  const now = Date.UTC(2026, 9, 27, 12);
  const request = requestFor({ ...defaults, q: 'db', range: 'week' }, now);
  assert.equal(request.date_from, now / 1000 - 7 * 86400);
  const sameDay = requestFor({ ...defaults, q: 'db', range: 'custom', from: '2026-10-27', to: '2026-10-27' });
  assert.equal(sameDay.date_to - sameDay.date_from, 86400);
  const onlyEnd = requestFor({ ...defaults, q: 'db', range: 'custom', to: '2026-10-27' });
  assert.equal(onlyEnd.date_from, undefined);
  for (const change of [{}, { from: '2026-02-30' }, { from: '1960-01-01' },
    { from: '2026-10-28', to: '2026-10-27' }]) {
    assert.throws(() => requestFor({ ...defaults, q: 'db', range: 'custom', ...change }));
  }
});
