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

// Read-only backend qualification. This is deliberately not an alternative
// passing path in evaluate-gate: provider completeness still needs live proof.
import {createHash} from 'node:crypto';
import {assertGateRun} from './gate-scope.mjs';
import {sanitizeTailEvent} from './metrics-tail.mjs';
import {DiagnosticCoverage} from './live-diagnostics.mjs';
import {rayIdentity,versionIdentity,requestAttemptIdentity,invocationTypes} from './invocation-accounting.mjs';

const roles = ['app', 'writer', 'reader', 'builder'];
const digest = value => createHash('sha256').update(value).digest('hex');
const fail = code => { throw Error(code); };

export function persistedWindowQuery(run, role, timeframe, {view = 'events', offset} = {}) {
  assertGateRun(run);
  if (!roles.includes(role) || !['events', 'calculations'].includes(view)) fail('InvalidDiagnosticView');
  const {from, to} = timeframe;
  if (!Number.isSafeInteger(from) || !Number.isSafeInteger(to) || from < 0 || from >= to || to - from > 60000) fail('InvalidDiagnosticWindow');
  if (offset !== undefined && (typeof offset !== 'string' || !/^[\w-]{1,256}$/.test(offset))) fail('InvalidDiagnosticCursor');
  return {
    queryId: crypto.randomUUID(), dry: true, view, timeframe: {from, to}, limit: 1000,
    ...(view === 'calculations' ? {chartType: 'aggregate'} : {}),
    ...(offset === undefined ? {} : {offset, offsetDirection: 'next'}),
    parameters: {filterCombination: 'and',
      filters: [{key: '$metadata.service', type: 'string', operation: 'eq', value: run.id + (role === 'app' ? '' : '-' + role)}],
      ...(view === 'calculations' ? {calculations: [{operator: 'count', alias: 'events'}]} : {}),
    },
  };
}

export function sanitizePersistedEvent(row, worker, timeframe) {
  const metadata = row?.$metadata, context = row?.$workers;
  if (!metadata || metadata.service !== worker || context?.scriptName && context.scriptName !== worker ||
      typeof metadata.id !== 'string' || !/^[\w-]{1,256}$/.test(metadata.id) ||
      !Number.isSafeInteger(row.timestamp) || row.timestamp < timeframe.from || row.timestamp > timeframe.to) fail('DiagnosticEventScopeMismatch');
  if (!['cf-worker-event', 'cf-worker', 'cf-worker-log'].includes(metadata.type)) fail('DiagnosticEventSchemaUnknown');
  const invocation = metadata.type === 'cf-worker-event';
  if (invocation && !['ok', 'exception', 'canceled', 'exceededMemory', 'exceededCpu'].includes(context?.outcome)) fail('DiagnosticOutcomeUnknown');
  const requestId = metadata.requestId ?? context?.requestId;
  if (typeof requestId !== 'string' || requestId.length < 1 || requestId.length > 256) fail('DiagnosticInvocationIdentityMissing');
  // All raw source/error text passes through the same allowlist as live tails.
  // Never copy HTTP context, arbitrary source keys, URLs or metadata to disk.
  const messages = [metadata.message, metadata.error];
  if (typeof row.source === 'string') messages.push(row.source);
  else if (row.source && typeof row.source === 'object') {
    messages.push(JSON.stringify(row.source));
    if(typeof row.source.message==='string')messages.push(row.source.message);
  }
  const event = sanitizeTailEvent({outcome: invocation ? context.outcome : undefined,
    cpuTime:context?.cpuTimeMs??context?.cpuTime,wallTime:context?.wallTimeMs??context?.wallTime,scriptVersion:context?.scriptVersion,
    truncated: context?.truncated === true,
    logs: [{timestamp: row.timestamp, message: messages.filter(v => typeof v === 'string')}],
    // Only the provider's error field is an exception. Scanning serialized
    // metric objects as error text misclassifies cpuTimeMs + memory_limit
    // field names as a CPU-limit failure.
    ...(typeof metadata.error==='string'?{exceptions:[{name:'Error',message:metadata.error}]}:{}),
  }) ?? {};
  return {event_id_sha256: digest(JSON.stringify([requestId,metadata.id])), invocation_id_sha256: digest(requestId), timestamp: row.timestamp, invocation, event,
    ray_id_sha256:rayIdentity(metadata.rayId),version_id:versionIdentity(context?.scriptVersion?.id),
    attempt_id_sha256:requestAttemptIdentity(metadata.url??context?.event?.request?.url??row.source?.request?.url),
    event_type:invocationTypes.has(context?.eventType)?context.eventType:'unknown'};
}

// Complete one small window with before/after aggregate reconciliation.
// A repeat after ingestion settlement must produce the same digest; even that
// does not establish that the platform stored every invocation (see design).
export async function collectPersistedWindow(run, role, token, timeframe, {fetch: request = fetch, signal, clock = Date.now} = {}) {
  persistedWindowQuery(run, role, timeframe);
  if (typeof token !== 'string' || !/^[A-Za-z0-9_-]{20,256}$/.test(token)) fail('InvalidObservabilityToken');
  const worker = run.id + (role === 'app' ? '' : '-' + role);
  const collected_started_at=clock();let queries = 0, transferred = 0;
  async function query(view, offset) {
    if (++queries > 64) fail('DiagnosticQueryBudgetExceeded');
    const remaining=collected_started_at+120000-clock();if(remaining<=0)fail('DiagnosticWindowDeadline');
    let response;
    try {
      response = await request('https://api.cloudflare.com/client/v4/accounts/' + run.account + '/workers/observability/telemetry/query', {
        method: 'POST', headers: {Authorization: 'Bearer ' + token, 'Content-Type': 'application/json'},
        body: JSON.stringify(persistedWindowQuery(run, role, timeframe, {view, offset})),
        signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(Math.min(30000,remaining))]) : AbortSignal.timeout(Math.min(30000,remaining)),
      });
    } catch { fail('DiagnosticQueryTransportFailed'); }
    if (!response.ok) { await response.body?.cancel(); fail('DiagnosticQueryStatus' + response.status); }
    let bytes = 0; const chunks = [];
    try{for await (const chunk of response.body) {
      bytes += chunk.byteLength; transferred += chunk.byteLength;
      if (bytes > 4 * 1024 * 1024 || transferred > 64 * 1024 * 1024) fail('DiagnosticQueryResponseExceeded');
      chunks.push(chunk);
    }}catch(error){fail(error.message==='DiagnosticQueryResponseExceeded'?error.message:'DiagnosticQueryReadFailed');}
    let value; try { value = JSON.parse(Buffer.concat(chunks).toString()); } catch { fail('DiagnosticQueryResponseMalformed'); }
    if (value.success !== true || value.result?.run?.status !== 'COMPLETED') fail('DiagnosticQueryIncomplete');
    const result = value.result;
    if (!result.statistics || (result.statistics.abr_level ?? 1) !== 1) fail('DiagnosticQuerySampled');
    return result;
  }
  async function count() {
    const result = await query('calculations'), calculations = result.calculations;
    if (!Array.isArray(calculations) || calculations.length !== 1 || calculations[0].alias !== 'events' || calculations[0].aggregates?.length !== 1) fail('DiagnosticCountSchemaUnknown');
    const aggregate = calculations[0].aggregates[0];
    if (!Number.isSafeInteger(aggregate.value) || aggregate.value < 0 || aggregate.value > 20000 || aggregate.count !== aggregate.value) fail('DiagnosticCountInvalid');
    // The live API reports zero average interval for no matched events. It
    // conveys no sampling/delivery evidence; pagination and the final count
    // still must independently confirm an empty stored set.
    if(aggregate.value===0&&aggregate.sampleInterval===0)return 0;
    if (aggregate.sampleInterval !== 1) throw Object.assign(Error('DiagnosticIngestionSampled'),{diagnostic:{sample_interval:Number.isFinite(aggregate.sampleInterval)?aggregate.sampleInterval:null,estimated_events:aggregate.value}});
    return aggregate.value;
  }
  const before = await count(), rows = [], seen = new Set(), invocations = new Set(); let offset;
  for (;;) {
    const result = await query('events', offset), page = result.events;
    if (!page || !Array.isArray(page.events) || page.events.length > 1000) fail('DiagnosticEventsSchemaUnknown');
    if (page.count !== before) fail('DiagnosticCountChanged');
    for (const row of page.events) {
      const safe = sanitizePersistedEvent(row, worker, timeframe);
      if (seen.has(safe.event_id_sha256)) fail('DiagnosticCursorRepeated');
      if(safe.invocation&&invocations.has(safe.invocation_id_sha256))fail('DiagnosticInvocationRepeated');
      if(safe.invocation)invocations.add(safe.invocation_id_sha256);
      seen.add(safe.event_id_sha256); rows.push(safe);
      if (rows.length > before) fail('DiagnosticCountMismatch');
    }
    if (rows.length === before) break;
    if (!page.events.length) fail('DiagnosticPaginationIncomplete');
    offset = page.events.at(-1).$metadata.id;
  }
  if (await count() !== before) fail('DiagnosticCountChanged');
  rows.sort((a, b) => a.event_id_sha256.localeCompare(b.event_id_sha256));
  const coverage = new DiagnosticCoverage([run]);
  for (const row of rows) coverage.observe(run.id, role, row.event);
  const observed = coverage.stream(run.id, role);
  return {schema: 3, qualification: false, worker, role, timeframe: {...timeframe},
    collected_started_at,collected_finished_at:clock(),
    reconciled: true, events: rows.length, invocations: rows.filter(row => row.invocation).length,
    invocation_errors:rows.filter(row=>row.invocation&&row.event.outcome!=='ok').length,
    resource_failures: observed.resource_failures, gap: observed.gap, queries, transferred_bytes: transferred,
    rows_sha256: digest(JSON.stringify(rows)), rows};
}
