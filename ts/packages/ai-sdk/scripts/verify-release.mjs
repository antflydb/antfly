import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Publication must pass the same strict, non-hoisted consumers as local CI,
// with every SDK dependency installed from its minimum admitted npm version.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const manifest = JSON.parse(readFileSync(resolve(root, "ts/packages/ai-sdk/package.json"), "utf8"));
let sdkVersion;
for (const [name, range] of Object.entries(manifest.dependencies)) {
  if (!range.startsWith("workspace:")) continue;
  if (range !== "workspace:^") throw new Error(`Unsupported release dependency range: ${range}`);
  const dependency = JSON.parse(
    readFileSync(resolve(root, `ts/packages/${name.split("/")[1]}/package.json`), "utf8")
  );
  const result = spawnSync("npm", ["view", `${name}@${dependency.version}`, "version", "--json"], {
    cwd: root,
    encoding: "utf8",
  });
  if (result.status !== 0 || JSON.parse(result.stdout) !== dependency.version)
    throw new Error(
      `Publish ${name}@${dependency.version} before @antfly/ai-sdk; its minimum admitted SDK version is not available from npm.`
    );
  if (name === "@antfly/sdk") sdkVersion = dependency.version;
}
if (!sdkVersion) throw new Error("Expected the SDK workspace dependency");
const result = spawnSync(
  process.execPath,
  [resolve(root, "ts/packages/ai-sdk/scripts/verify-package.mjs"), "--registry-sdk"],
  {
    cwd: root,
    stdio: "inherit",
    env: { ...process.env, ANTFLY_SDK_VERSION: sdkVersion },
  }
);
if (result.status !== 0) throw new Error("Strict registry-only package consumers failed");
