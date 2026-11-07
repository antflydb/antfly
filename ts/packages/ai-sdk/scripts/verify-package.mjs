import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const registrySDK = process.argv.includes("--registry-sdk");
const registrySDKVersion = process.env.ANTFLY_SDK_VERSION ?? "0.2.2";
const packedNames = registrySDK ? ["ai-sdk"] : ["sdk", "ai-sdk"];
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
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

for (const name of packedNames)
  run(
    "pnpm",
    ["pack", "--out", resolve(artifacts, `${name}.tgz`)],
    resolve(root, `ts/packages/${name}`)
  );

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
  "ai@7.0.162",
  "zod@4.6.7",
  "typescript@5.9.3",
  "@types/node@25.9.3",
]);

for (const [extension, statement] of [
  ["mjs", "import * as integration from '@antfly/ai-sdk';"],
  ["cjs", "const integration = require('@antfly/ai-sdk');"],
]) {
  writeFileSync(
    resolve(consumer, `exports.${extension}`),
    `${statement}\nfor (const key of ['createAntfly','createMemoryTools','createSearchTool','createGraphTool','createSqlTool','createChatHistory','AntflyChatLanguageModel','AntflyEmbeddingModel','AntflyRerankingModel']) { if (typeof integration[key] !== 'function') throw new Error(key); }\n`
  );
  run("node", [`exports.${extension}`]);
}

const contract = `import type { ProviderV4 } from '@ai-sdk/provider';
import { AntflyClient, InferenceClient } from '@antfly/sdk';
import { createAntfly, createMemoryTools, createSearchTool, createGraphTool, createSqlTool, createChatHistory } from '@antfly/ai-sdk';
const provider: ProviderV4 = createAntfly({ baseUrl: 'http://localhost:8080' });
void provider.imageModel;
const client = new AntflyClient({ baseUrl: 'http://localhost:8080' });
void createMemoryTools({ client, space: 'test' });
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
      ai: "7.0.162",
      zod: "4.6.7",
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
const provider = createAntfly({baseUrl:'http://localhost:8080'});
const language: LanguageModel = provider.languageModel('model');
const embedding: EmbeddingModel = provider.embeddingModel('model');
void language; void embedding; void uploadFile;
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
