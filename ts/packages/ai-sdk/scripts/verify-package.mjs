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

const receipt = {
  consumer,
  node: process.version,
  sdk: { source: registrySDK ? "npm-registry" : "local-tarball", version: installed.version },
  esm: "passed",
  commonjs: "passed",
  types: "passed",
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
