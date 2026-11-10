import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const registrySDK = process.argv.includes("--registry-sdk");
const sdkManifest = JSON.parse(
  readFileSync(new URL("../../sdk/package.json", import.meta.url), "utf8")
);
const registrySDKVersion = process.env.ANTFLY_SDK_VERSION ?? sdkManifest.version;
const packedNames = registrySDK ? ["ai-sdk"] : ["sdk", "ai-sdk"];
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const manifest = JSON.parse(readFileSync(resolve(root, "ts/packages/ai-sdk/package.json"), "utf8"));
const latestPeers = process.argv.includes("--latest-peers");
function peerVersion(name) {
  const range = manifest.peerDependencies[name];
  if (!/^\^\d+\.\d+\.\d+$/.test(range)) throw new Error(`Expected a caret peer range for ${name}`);
  if (!latestPeers) return range.slice(1);
  const values = JSON.parse(run("npm", ["view", `${name}@${range}`, "version", "--json"], root));
  return Array.isArray(values) ? values.at(-1) : values;
}
const state = resolve(root, ".state/ai-sdk");
mkdirSync(state, { recursive: true });
const consumer = mkdtempSync(resolve(state, "consumer-"));
const artifacts = resolve(consumer, "artifacts");
mkdirSync(artifacts);

function run(command, args, cwd = consumer) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, pnpm_config_verify_deps_before_run: "false" },
  });
  if (result.status !== 0)
    throw new Error(`${command} ${args.join(" ")} failed:\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

const aiVersion = peerVersion("ai");
const zodVersion = peerVersion("zod");

for (const name of packedNames)
  run(
    "pnpm",
    ["pack", "--out", resolve(artifacts, `${name}.tgz`)],
    resolve(root, `ts/packages/${name}`)
  );

const packedManifest = JSON.parse(
  run("tar", ["-xOf", resolve(artifacts, "ai-sdk.tgz"), "package/package.json"])
);
if (packedManifest.dependencies["@antfly/sdk"] !== `^${sdkManifest.version}`)
  throw new Error("Packed SDK dependency floor differs from the tested minimum");

const entries = run("tar", ["-tzf", resolve(artifacts, "ai-sdk.tgz")])
  .trim()
  .split("\n");
if (entries.some((path) => !/^package\/(dist\/.*|README\.md|LICENSE|package\.json)$/.test(path)))
  throw new Error(`Unexpected package contents: ${entries.join(",")}`);

writeFileSync(resolve(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
run("npm", [
  "install",
  "--ignore-scripts",
  "--no-audit",
  "--no-fund",
  registrySDK ? `@antfly/sdk@${registrySDKVersion}` : resolve(artifacts, "sdk.tgz"),
  resolve(artifacts, "ai-sdk.tgz"),
  `ai@${aiVersion}`,
  `zod@${zodVersion}`,
  "typescript@5.9.3",
  "@types/node@25.9.3",
]);

for (const [extension, statement] of [
  [
    "mjs",
    "import * as integration from '@antfly/ai-sdk'; import * as sdk from '@antfly/sdk'; import { StandaloneSourcesClient } from '@antfly/sdk/sources';",
  ],
  [
    "cjs",
    "const integration = require('@antfly/ai-sdk'); const sdk = require('@antfly/sdk'); const { StandaloneSourcesClient } = require('@antfly/sdk/sources');",
  ],
]) {
  writeFileSync(
    resolve(consumer, `exports.${extension}`),
    `${statement}\nfor(const name of ['AntflyHTTPError','AntflyResponseError','AntflyClient','InferenceTransportError']) if(typeof sdk[name] !== 'function') throw new Error('SDK foundation missing: '+name);\nif(typeof StandaloneSourcesClient !== 'function') throw new Error('SDK sources subpath missing');\nfor (const key of ['createAntfly','createMemoryTools','createMemorySession','createSearchTool','createGraphTool','createSqlTool','createChatHistory','AntflyChatLanguageModel','AntflyEmbeddingModel','AntflyRerankingModel']) { if (typeof integration[key] !== 'function') throw new Error(key); }\n`
  );
  const runtimeCheck = `
async function verifyEmbeddingBatching() {
  const { embedMany } = await import('ai');
  const batches = [];
  globalThis.fetch = async request => {
    const { input } = await request.json();
    batches.push(input);
    if (input.length > 2 || input.reduce((n, value) => n + Buffer.byteLength(value), 0) > 5)
      throw new Error('Embedding bounds were not honored by the AI SDK');
    return Response.json({ data: input.map((value, index) => ({ index, embedding: [value.length] })) });
  };
  const provider = integration.createAntfly({ baseUrl: 'http://inference.test', embedding: { maxEmbeddingsPerCall: 2, maxInputBytesPerCall: 5 } });
  const result = await embedMany({ model: provider.embeddingModel('model'), values: ['é', 'é', 'é', 'a'], maxRetries: 0 });
  if (result.embeddings.length !== 4 || batches.length < 2) throw new Error('Embedding batch result incomplete');
  if (typeof new sdk.InferenceClient({ baseUrl: 'http://inference.test' }).openGenerateStream !== 'function')
    throw new Error('SDK eager streaming API missing');
}
verifyEmbeddingBatching().catch(error => { console.error(error); process.exitCode = 1; });
`;
  writeFileSync(
    resolve(consumer, `exports.${extension}`),
    readFileSync(resolve(consumer, `exports.${extension}`), "utf8") + runtimeCheck
  );
  run("node", [`exports.${extension}`]);
}

const contract = `import type { ProviderV4 } from '@ai-sdk/provider';
import { AntflyClient, InferenceClient } from '@antfly/sdk';
import { createAntfly, createMemoryTools, createMemorySession, createSearchTool, createGraphTool, createSqlTool, createChatHistory } from '@antfly/ai-sdk';
const provider: ProviderV4 = createAntfly({ baseUrl: 'http://localhost:8080' });
void provider.imageModel;
const client = new AntflyClient({ baseUrl: 'http://localhost:8080' });
void createMemoryTools({ client, space: 'test' });
const memory = createMemorySession({ client, space: 'test', runId: 'host-run', profile: 'agent' });
void memory.loadBrief;
void memory.recordAccess;
void memory.recordOutcome;
void createSearchTool({ client, table: 'test' });
void createGraphTool({ client, table: 'test' });
void createSqlTool({ client });
void createChatHistory({ client, table: 'test' });
void new InferenceClient({ baseUrl: 'http://localhost:8080' });
`;
for (const extension of ["mts", "cts"]) {
  writeFileSync(resolve(consumer, `contract.${extension}`), contract);
  run("node", [
    "node_modules/typescript/bin/tsc",
    "--noEmit",
    "--strict",
    "--skipLibCheck",
    "--target",
    "ES2022",
    "--module",
    "NodeNext",
    "--moduleResolution",
    "NodeNext",
    `contract.${extension}`,
  ]);
}

const installed = JSON.parse(
  readFileSync(resolve(consumer, "node_modules/@antfly/sdk/package.json"), "utf8")
);
if (registrySDK && installed.version !== registrySDKVersion)
  throw new Error("Registry SDK version mismatch");

// npm's hoisted layout can hide undeclared imports in published .d.ts files.
// Repeat the export and type checks in a consumer that exposes only declared
// dependencies. Keep consumer imports limited to packages it actually declares.
// Keep this outside the npm consumer so parent node_modules cannot hide
// missing declarations in the non-hoisted dependency graph.
const isolated = mkdtempSync(resolve(state, "isolated-consumer-"));
const sdkDependency = registrySDK ? registrySDKVersion : `file:${resolve(artifacts, "sdk.tgz")}`;
writeFileSync(
  resolve(isolated, "package.json"),
  JSON.stringify({
    private: true,
    type: "module",
    dependencies: {
      "@antfly/sdk": sdkDependency,
      "@antfly/ai-sdk": `file:${resolve(artifacts, "ai-sdk.tgz")}`,
      ai: aiVersion,
      zod: zodVersion,
      typescript: "5.9.3",
      "@types/node": "25.9.3",
      // @ai-sdk/provider's own json-schema dependency omits its typings. Supply
      // those upstream typings so skipLibCheck can remain disabled here.
      "@types/json-schema": "7.0.15",
    },
  })
);
writeFileSync(
  resolve(isolated, "pnpm-workspace.yaml"),
  `hoist: false\nautoInstallPeers: true\noverrides:\n  '@antfly/sdk': ${JSON.stringify(sdkDependency)}\n`
);
run("pnpm", ["install", "--ignore-scripts", "--no-frozen-lockfile"], isolated);
const isolatedContract = `import type { LanguageModel, EmbeddingModel } from 'ai';
import { createAntfly, createSearchTool, uploadFile } from '@antfly/ai-sdk';
import { StandaloneSourcesClient } from '@antfly/sdk/sources';
const provider = createAntfly({baseUrl:'http://localhost:8080'});
const language: LanguageModel = provider.languageModel('model');
const embedding: EmbeddingModel = provider.embeddingModel('model');
void language; void embedding; void uploadFile;
void new StandaloneSourcesClient({baseUrl:'http://localhost:8082',token:'test-token'});
void createSearchTool({client:provider.client,table:'docs'});
`;
for (const extension of ["mts", "cts"]) {
  writeFileSync(resolve(isolated, `contract.${extension}`), isolatedContract);
  run(
    "node",
    [
      "node_modules/typescript/bin/tsc",
      "--noEmit",
      "--strict",
      "--target",
      "ES2022",
      "--module",
      "NodeNext",
      "--moduleResolution",
      "NodeNext",
      `contract.${extension}`,
    ],
    isolated
  );
}
for (const extension of ["mjs", "cjs"]) {
  writeFileSync(
    resolve(isolated, `exports.${extension}`),
    readFileSync(resolve(consumer, `exports.${extension}`))
  );
  run("node", [`exports.${extension}`], isolated);
}

const receipt = {
  consumer,
  node: process.version,
  peers: {
    selection: latestPeers ? "latest-compatible" : "minimum",
    ai: aiVersion,
    zod: zodVersion,
  },
  sdk: { source: registrySDK ? "npm-registry" : "local-tarball", version: installed.version },
  esm: "passed",
  commonjs: "passed",
  types: "passed",
  isolated: {
    hoisting: false,
    skipLibCheck: false,
    types: "passed",
    esm: "passed",
    commonjs: "passed",
  },
  artifacts: Object.fromEntries(
    packedNames.map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(resolve(artifacts, `${name}.tgz`)))
        .digest("hex"),
    ])
  ),
};
writeFileSync(resolve(consumer, "receipt.json"), JSON.stringify(receipt, null, 2));
console.log(JSON.stringify(receipt, null, 2));
