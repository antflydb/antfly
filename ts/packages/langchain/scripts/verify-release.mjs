import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Unlike verify-package, only the release target is local. All dependencies
// must be installable from npm; this gate must precede publication.
const target = process.argv[2];
if (!["langchain", "langgraph-store"].includes(target))
  throw new Error("Specify langchain or langgraph-store");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const state = resolve(root, ".state/langchain");
mkdirSync(state, { recursive: true });
const consumer = mkdtempSync(resolve(state, "release-consumer-"));
function run(command, args, cwd = consumer) {
  const result = spawnSync(command, args, {
    cwd,
    encoding: "utf8",
    env: { ...process.env, pnpm_config_verify_deps_before_run: "false" },
  });
  if (result.status !== 0)
    throw new Error(
      `Registry-only release check failed: ${command} ${args.join(" ")}\n${result.stdout}\n${result.stderr}\nPublish the SDK foundations, then langchain, then langgraph-store. Local dependency tarballs are deliberately excluded.`
    );
  return result.stdout;
}
const directory = resolve(root, `ts/packages/${target}`);
const manifest = JSON.parse(readFileSync(resolve(directory, "package.json"), "utf8"));
for (const [name, range] of Object.entries(manifest.dependencies)) {
  if (!range.startsWith("workspace:")) continue;
  const dependency = JSON.parse(
    readFileSync(resolve(root, `ts/packages/${name.split("/")[1]}/package.json`), "utf8")
  );
  run("npm", ["view", `${name}@^${dependency.version}`, "version", "--json"]);
}
const artifact = resolve(consumer, `${target}.tgz`);
run("pnpm", ["pack", "--out", artifact], directory);
writeFileSync(resolve(consumer, "package.json"), JSON.stringify({ private: true, type: "module" }));
run("npm", [
  "install",
  "--ignore-scripts",
  "--no-audit",
  "--no-fund",
  artifact,
  "typescript@5.9.3",
  "@types/node@25.9.3",
]);
for (const extension of ["mjs", "cjs"]) {
  const imports =
    extension === "mjs"
      ? `import * as sdk from '@antfly/sdk'; import * as integration from '@antfly/${target}';`
      : `const sdk = require('@antfly/sdk'); const integration = require('@antfly/${target}');`;
  writeFileSync(
    resolve(consumer, `exports.${extension}`),
    `${imports}\nfor(const name of ['AntflyHTTPError','AntflyResponseError','AntflyClient']) if(typeof sdk[name] !== 'function') throw new Error('SDK foundation missing: '+name); if(typeof integration.${target === "langchain" ? "AntflyVectorStore" : "AntflyStore"} !== 'function') throw new Error('Integration export missing');`
  );
  run("node", [`exports.${extension}`]);
}
writeFileSync(
  resolve(consumer, "contract.mts"),
  `import type { EmbedOptions } from '@antfly/sdk'; const options: EmbedOptions = {task_type:'RETRIEVAL_DOCUMENT',error_policy:'fail_fast',signal:new AbortController().signal}; void options;`
);
run("node", [
  "node_modules/typescript/bin/tsc",
  "--noEmit",
  "--skipLibCheck",
  "--module",
  "NodeNext",
  "--moduleResolution",
  "NodeNext",
  "--target",
  "ES2022",
  "contract.mts",
]);
console.log(
  JSON.stringify(
    { target, consumer, dependencies: "npm-registry-only", exports: "passed", types: "passed" },
    null,
    2
  )
);
