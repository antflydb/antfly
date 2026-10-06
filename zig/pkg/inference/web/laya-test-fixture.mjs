// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
// Tiny deterministic checkpoint exercises the real streamed WASM adapter.
export function layaFixture(extra = {}, precision = 'fp32') {
  const h = 64, f = 96, vocab = ['[PAD]', '[UNK]', '[CLS]', '[SEP]', '[MASK]', 'state', 'question', 'choice', 'score', 'noul', ':', 'input', 'yes', 'no', 'false', 'true', 'low', 'medium', 'high', 'search', 'fetch', 'none'];
  const config = { model_type: 'modernbert', hidden_size: h, num_hidden_layers: 2, num_attention_heads: 2, intermediate_size: f, vocab_size: 32, max_position_embeddings: 512, local_attention: 8, global_attn_every_n_layers: 2, layer_norm_eps: 1e-5, pad_token_id: 0, cls_token_id: 2, sep_token_id: 3, laya: { head_layers: 1, max_len: 512, head_max_len: 192, mask_token: '[MASK]', ...extra } };
  const tensors = {}, chunks = []; let offset = 0, seed = 17;
  const add = (name, shape, norm = false) => {
    const data = new Float32Array(shape.reduce((a, b) => a * b, 1));
    for (let i = 0; i < data.length; i++) {
      seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
      data[i] = norm ? 1 : name.endsWith('bias') ? 0 : ((seed / 2 ** 32) - .5) * .15;
    }
    let stored = precision === 'fp16' && !norm && !name.endsWith('bias') ? new Float16Array(data) : data;
    if (precision === 'bf16' && !norm && !name.endsWith('bias')) {
      stored = new Uint16Array(data.length);
      const bits = new Uint32Array(data.buffer);
      for (let i = 0; i < data.length; i++) stored[i] = (bits[i] + 0x7fff + ((bits[i] >>> 16) & 1)) >>> 16;
    }
    tensors[name] = { dtype: stored instanceof Uint16Array ? 'BF16' : stored instanceof Float16Array ? 'F16' : 'F32', shape, data_offsets: [offset, offset + stored.byteLength] };
    chunks.push(stored); offset += stored.byteLength;
  };
  const pair = (name, input, output) => { add(name + '.weight', [output, input]); add(name + '.bias', [output]); };
  const norm = (name, bias = true) => { add(name + '.weight', [h], true); if (bias) add(name + '.bias', [h]); };
  add('encoder.embeddings.tok_embeddings.weight', [32, h]); norm('encoder.embeddings.norm', false); norm('encoder.final_norm', false);
  for (let i = 0; i < 2; i++) {
    const p = `encoder.layers.${i}`;
    if (i) norm(p + '.attn_norm', false);
    norm(p + '.mlp_norm', false);
    for (const [name, shape] of [['attn.Wqkv', [3 * h, h]], ['attn.Wo', [h, h]], ['mlp.Wi', [2 * f, h]], ['mlp.Wo', [h, f]]]) add(p + '.' + name + '.weight', shape);
  }
  if (extra.format === 'opendecider') {
    config.laya.head_layers = 0; pair('scorer.0', h, h); norm('scorer.2'); pair('scorer.3', h, 1);
  } else {
    add('type_emb.weight', [3, h]); norm('scorer.0'); pair('scorer.1', h, h); pair('scorer.3', h, 1);
    pair('act_head.0', h + 4, 256); pair('act_head.2', 256, 2);
    const p = 'head.layers.0'; add(p + '.self_attn.in_proj_weight', [3 * h, h]); add(p + '.self_attn.in_proj_bias', [3 * h]);
    pair(p + '.self_attn.out_proj', h, h); pair(p + '.linear1', h, 4 * h); pair(p + '.linear2', 4 * h, h); norm(p + '.norm1'); norm(p + '.norm2');
    if (extra.decision_head === 'pointer') { norm('pointer.norm'); pair('pointer.q', h, extra.pointer_dim ?? 32); pair('pointer.k', h, extra.pointer_dim ?? 32); }
  }
  const header = new TextEncoder().encode(JSON.stringify(tensors)), prefix = new ArrayBuffer(8); new DataView(prefix).setBigUint64(0, BigInt(header.length), true);
  const tokenizer = { version: '1.0', truncation: null, padding: null, added_tokens: vocab.slice(0, 5).map((content, id) => ({ id, content, special: true, single_word: false, lstrip: false, rstrip: false, normalized: false })), normalizer: { type: 'BertNormalizer', clean_text: true, handle_chinese_chars: true, strip_accents: null, lowercase: true }, pre_tokenizer: { type: 'BertPreTokenizer' }, model: { type: 'WordPiece', unk_token: '[UNK]', continuing_subword_prefix: '##', max_input_chars_per_word: 100, vocab: Object.fromEntries(vocab.map((t, i) => [t, i])) }, post_processor: null, decoder: { type: 'WordPiece', prefix: '##', cleanup: true } };
  return new Map([['config.json', new Blob([JSON.stringify(config)])], ['tokenizer_config.json', new Blob(['{"mask_token":"[MASK]"}'])], ['tokenizer.json', new Blob([JSON.stringify(tokenizer)])], ['model.safetensors', new Blob([prefix, header, ...chunks])]]);
}
export const layaRequest = { schema_version: 2, model: 'tiny-laya', inputs: [{ id: 'state', content: 'state state state' }], schema: { classifications: [
  { name: 'tool', instruction: 'question', mode: 'single', labels: ['search', 'fetch', 'none'] },
  { name: 'urgency', instruction: 'question', mode: 'ordinal', labels: ['low', 'medium', 'high'] },
  { name: 'needed', instruction: 'question', mode: 'boolean', labels: ['false', 'true'] },
] } };
