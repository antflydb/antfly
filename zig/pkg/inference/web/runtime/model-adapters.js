// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
// Internal adapters use configuration contracts, never repository/display names.
// Task/capability strings are Antfly's existing model metadata vocabulary.
const limits = Object.freeze({ maxInputs: 1, maxTextBytes: 256 * 1024, maxRequestBytes: 512 * 1024, maxSchemaBytes: 64 * 1024 });
const trainedKinds = Object.freeze(['choice', 'score', 'predicate']);
const adapters = [
  {
    id: 'laya', family: c => c.laya.format === 'opendecider' ? 'opendecider' : 'laya', architecture: 'modernbert',
    matches: c => ['modernbert', 'modern_bert'].includes(c.model_type) && c.laya && typeof c.laya === 'object',
    tasks: ['decide'], capabilities: ['typed_decisions'], kinds: trainedKinds,
    unavailable: c => c.vocab_size > 262144 || c.hidden_size > 1024 || c.num_hidden_layers > 32 || c.intermediate_size > 4096 || c.laya.max_len > 2048
      ? 'This Laya encoder exceeds the browser vocabulary, geometry, or sequence budget.' : undefined,
  },
  {
    id: 'boundary', family: () => 'gliner25', architecture: 'boundary',
    matches: c => c.architecture === 'boundary', tasks: ['extract', 'decide'],
    capabilities: ['extraction', 'classification', 'relations', 'typed_decisions'], kinds: trainedKinds,
    unavailable: (_c, e) => e.hidden_size > 2048 || e.num_hidden_layers > 48 || e.intermediate_size > 8192 ? 'This boundary encoder exceeds the browser geometry budget.' : undefined,
  },
  {
    id: 'decide', family: () => 'decide', architecture: 'span',
    matches: c => c.model_type === 'extractor' && !c.boundary_head && c.architecture === 'span' && c.config_version === 3 && c.architecture_version === 1 && c.span_head?.span_mode === 'markerV0',
    tasks: ['extract', 'decide'], capabilities: ['classification', 'typed_decisions'], kinds: trainedKinds,
    unavailable: (_c, e) => e.hidden_size > 2048 || e.num_hidden_layers > 48 || e.intermediate_size > 8192 || e.vocab_size > 262144 ? 'This marker encoder exceeds the browser geometry budget.' : undefined,
  },
  {
    id: 'span', family: () => 'gliner2', architecture: 'span',
    matches: c => c.model_type === 'extractor' && !c.boundary_head && (!c.config_version || c.config_version < 3),
    tasks: ['extract'], capabilities: ['extraction', 'classification', 'relations'], kinds: [],
    unavailable: (_c, e) => e.hidden_size !== 768 || e.num_hidden_layers !== 12 || e.vocab_size !== 128011
      ? 'This legacy span encoder geometry has no browser weight adapter yet.' : undefined,
  },
  {
    id: 'embedding_similarity', family: () => 'embedding', architecture: 'embedding',
    matches: (c, _e, m) => ['embedding_gemma2', 'embedding_gemma2_text'].includes(c.model_type) || m?.capabilities?.includes('embedding_similarity'),
    tasks: ['decide'], capabilities: ['embedding_similarity'], kinds: ['choice', 'multi_choice'],
    unavailable: c => !['embedding_gemma2', 'embedding_gemma2_text'].includes(c.model_type) ? 'No embedding-similarity encoder adapter matches this configuration.' : undefined,
  },
];
export function resolveAdapter(config, encoderConfig = config, manifest) {
  const adapter = adapters.find(a => a.matches(config, encoderConfig, manifest));
  if (!adapter) throw new Error('Unsupported inference architecture');
  const reason = adapter.unavailable(config, encoderConfig);
  let declaredTasks = manifest?.tasks ?? adapter.tasks;
  let declaredCapabilities = manifest?.capabilities ?? adapter.capabilities;
  for (const values of [declaredTasks, declaredCapabilities])
    if (!Array.isArray(values) || values.some(v => typeof v !== 'string')) throw new Error('Invalid model capability metadata');
  // Match Antfly's config-owned role correction for older imported manifests.
  // Laya's typed head is a decider even when upstream declares extract/classify;
  // embedding similarity must never inherit trained-head capabilities.
  if (adapter.id === 'laya' && declaredCapabilities.includes('typed_decisions')) {
    declaredTasks = [...new Set([...declaredTasks.filter(t => !['extract', 'classify'].includes(t)), 'decide'])];
    declaredCapabilities = declaredCapabilities.filter(c => !['classification', 'extraction'].includes(c));
  }
  if (adapter.id === 'embedding_similarity') {
    declaredTasks = [...new Set([...declaredTasks.filter(t => !['extract', 'classify'].includes(t)), 'embed', 'decide'])];
    declaredCapabilities = [...new Set([...declaredCapabilities.filter(c => !['classification', 'extraction', 'typed_decisions'].includes(c)), 'embedding_similarity'])];
  }
  const tasks = adapter.tasks.filter(t => declaredTasks.includes(t) && (t === 'decide'
    ? declaredCapabilities.includes(adapter.id === 'embedding_similarity' ? 'embedding_similarity' : 'typed_decisions')
    : adapter.capabilities.some(c => !['typed_decisions', 'embedding_similarity'].includes(c) && declaredCapabilities.includes(c))));
  const capabilities = adapter.capabilities.filter(c => declaredCapabilities.includes(c) && tasks.includes(['typed_decisions', 'embedding_similarity'].includes(c) ? 'decide' : 'extract'));
  return {
    adapter: adapter.id, family: adapter.family(config), architecture: adapter.architecture,
    tasks: [...declaredTasks], capabilities: [...declaredCapabilities],
    execution: { tasks: reason ? [] : tasks, capabilities: reason ? [] : capabilities, decisionKinds: !reason && tasks.includes('decide') ? [...adapter.kinds] : [], limits },
    availability: { available: !reason, ...(reason ? { reason, code: 'UNSUPPORTED_ARCHITECTURE' } : {}) },
  };
}
export async function inspectModel(input, precision, signal) {
  const { normalizeFiles, readJson, readModelManifest } = await import('./extraction-bundle.js');
  const files = normalizeFiles(input);
  const upstreamLaya = !files.has('config.json') && files.has('rl_agent_config.json');
  let config = await readJson(files, upstreamLaya ? 'encoder/config.json' : 'config.json');
  if (upstreamLaya) config = { ...config, laya: await readJson(files, 'rl_agent_config.json') };
  const manifest = await readModelManifest(files);
  const encoder = config.laya || ['embedding_gemma2', 'embedding_gemma2_text'].includes(config.model_type) || manifest?.capabilities?.includes('embedding_similarity') ? config : await readJson(files, 'encoder_config/config.json');
  const result = resolveAdapter(config, encoder, manifest);
  if (result.availability.available) {
    const { configurationReason } = await import('./model-configuration.js');
    const reason = await configurationReason(files, config, encoder, precision, signal);
    if (reason) {
      result.availability = { available: false, reason, code: 'UNSUPPORTED_ARCHITECTURE' };
      result.execution = { ...result.execution, tasks: [], capabilities: [], decisionKinds: [] };
    }
  }
  return result;
}
