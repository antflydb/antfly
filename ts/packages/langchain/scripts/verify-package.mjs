import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const state = resolve(root, ".state/langchain");
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
for (const name of ["sdk", "langchain", "langgraph-store"])
  run(
    "pnpm",
    ["pack", "--out", resolve(artifacts, `${name}.tgz`)],
    resolve(root, `ts/packages/${name}`)
  );
for (const name of ["langchain", "langgraph-store"]) {
  const entries = run("tar", ["-tzf", resolve(artifacts, `${name}.tgz`)])
    .trim()
    .split("\n");
  if (entries.some((path) => !/^package\/(dist\/.*|README\.md|LICENSE|package\.json)$/.test(path)))
    throw new Error("Unexpected package contents: " + entries.join(","));
}
const core =
  process.env.LANGCHAIN_CORE_VERSION ??
  createRequire(import.meta.url)("@langchain/core/package.json").version;
writeFileSync(resolve(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
run("npm", [
  "install",
  "--ignore-scripts",
  "--no-audit",
  "--no-fund",
  resolve(artifacts, "sdk.tgz"),
  resolve(artifacts, "langchain.tgz"),
  resolve(artifacts, "langgraph-store.tgz"),
  "@langchain/langgraph@1.5.3",
  `@langchain/core@${core}`,
  "typescript@5.9.3",
  "@types/node@25.9.3",
]);
for (const [extension, statement] of [
  ["mjs", "import * as integration from '@antfly/langchain';"],
  ["cjs", "const integration = require('@antfly/langchain');"],
]) {
  writeFileSync(
    resolve(consumer, `exports.${extension}`),
    `${statement}\nfor (const key of ['AntflyVectorStore','AntflyEmbeddings','AntflyRetriever','AntflyReranker','WriteOutcomeError','AntflyJSONStore','AntflyDocumentStore','AntflyByteStore','AntflyGraphStore','AntflyMultimodalCorpus','createAntflyGraphTools']) { if (typeof integration[key] !== 'function') throw new Error(key); }\n`
  );
  run("node", [`exports.${extension}`]);
}
for (const extension of ["mjs", "cjs"]) {
  writeFileSync(
    resolve(consumer, `memory.${extension}`),
    extension === "mjs"
      ? "import { AntflyStore } from '@antfly/langgraph-store'; if (typeof AntflyStore !== 'function') throw new Error('memory export');"
      : "const { AntflyStore } = require('@antfly/langgraph-store'); if (typeof AntflyStore !== 'function') throw new Error('memory export');"
  );
  run("node", [`memory.${extension}`]);
}
const contract = `import { AntflyStore } from '@antfly/langgraph-store';
import { AntflyClient, InferenceClient } from '@antfly/sdk';
import { AntflyEmbeddings, AntflyVectorStore, AntflyRetriever, AntflyReranker } from '@antfly/langchain';
const client = new AntflyClient({baseUrl:'http://localhost:8080'});
const embeddings = new AntflyEmbeddings({client:new InferenceClient({baseUrl:'http://localhost:8080'}),model:'test'});
const store = new AntflyVectorStore(embeddings,{client,table:'test'});
const retriever = new AntflyRetriever({vectorStore:store});
void retriever.invoke('question',{signal:new AbortController().signal});
void store.asRetriever({searchType:'mmr',k:2});
void new AntflyReranker({client:new InferenceClient({baseUrl:'http://localhost:8080'}),model:'test'}).asRunnable();
const memory = new AntflyStore({client,table:'memory'});
void memory.put(['users','alice'],'key',{text:'hello'});
// @ts-expect-error invalid mode must fail the public consumer contract
new AntflyRetriever({client,table:'test',mode:'invented'});
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
if (process.env.ANTFLY_URL) {
  writeFileSync(
    resolve(consumer, "live.mjs"),
    `import {AntflyClient, InferenceClient} from '@antfly/sdk';
import {AntflyEmbeddings, AntflyVectorStore, AntflyRetriever} from '@antfly/langchain';
import {randomUUID} from 'node:crypto';
const headers=process.env.ANTFLY_TOKEN?{Authorization:'Bearer '+process.env.ANTFLY_TOKEN}:{};
const client=new AntflyClient({baseUrl:process.env.ANTFLY_URL,headers});
const embeddings=new AntflyEmbeddings({client:new InferenceClient({baseUrl:process.env.ANTFLY_URL,headers}),model:process.env.ANTFLY_EMBEDDING_MODEL});
const table='lcjs_package_'+randomUUID().replaceAll('-','');
try {
const store=await AntflyVectorStore.fromDocuments([{id:'owner',pageContent:'Mira Chen owns operations.',metadata:{source:'package'}}],embeddings,{client,table});
const found=await new AntflyRetriever({vectorStore:store,k:1}).invoke('Who owns operations?');
if(found[0]?.id!=='owner') throw new Error('Installed-package retrieval failed');
const { AntflyStore } = await import('@antfly/langgraph-store');
const { AntflyJSONStore } = await import('@antfly/langchain');
const memory = new AntflyStore({client,table});
await memory.put(['users','alice'],'preference',{food:'apples'});
if ((await new AntflyStore({client,table}).get(['users','alice'],'preference'))?.value.food !== 'apples') throw new Error('Installed memory persistence failed');
const jsonStore = new AntflyJSONStore({client,table,namespace:'package'});
await jsonStore.mset([['record',{ok:true}]]);
if (!(await jsonStore.mget(['record']))[0]?.ok) throw new Error('Installed JSON store failed');
await store.delete({ids:['owner']});
if((await store.getByIds(['owner'])).length) throw new Error('Installed-package deletion failed');
} finally {await client.tables.drop(table);}
`
  );
  run("node", ["live.mjs"]);
}
const receipt = {
  consumer,
  node: process.version,
  core,
  esm: "passed",
  commonjs: "passed",
  types: "passed",
  live: process.env.ANTFLY_URL ? "passed" : "not requested",
  artifacts: Object.fromEntries(
    ["sdk", "langchain", "langgraph-store"].map((name) => [
      name,
      createHash("sha256")
        .update(readFileSync(resolve(artifacts, `${name}.tgz`)))
        .digest("hex"),
    ])
  ),
};
writeFileSync(resolve(consumer, "receipt.json"), JSON.stringify(receipt, null, 2));
console.log(JSON.stringify(receipt, null, 2));
