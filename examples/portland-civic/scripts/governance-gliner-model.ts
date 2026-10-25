// Native Antfly model bundle. Older `inference pull` builds omit encoder_config/.
// No Python runtime or conversion is needed: Antfly reads the original tensors.
import { mkdir, rename, writeFile, access } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
const revision = "78cea040597df251eedefa9d7ee2a756af39fe64";
const root =
  process.env.ANTFLY_MODELS_DIR || join(homedir(), ".antfly/inference/models");
const destination = join(root, "fastino/gliner2.5-base-v1");
const staging = `${destination}.download`;
try {
  await access(join(destination, "model_manifest.json"));
  console.log(`Native GLiNER2.5 already installed: ${destination}`);
} catch {
  await mkdir(join(staging, "encoder_config"), { recursive: true });
  for (const file of [
    "config.json",
    "encoder_config/config.json",
    "tokenizer.json",
    "tokenizer_config.json",
    "model.safetensors",
  ]) {
    console.log(`Downloading GLiNER2.5 ${file}`);
    execFileSync(
      "curl",
      [
        "--fail",
        "--location",
        "--retry",
        "3",
        "--silent",
        "--show-error",
        `https://huggingface.co/fastino/gliner2.5-base-v1/resolve/${revision}/${file}`,
        "--output",
        join(staging, file),
      ],
      { stdio: "inherit" },
    );
  }
  await writeFile(
    join(staging, "model_manifest.json"),
    JSON.stringify({
      type: "recognizer",
      tasks: ["extract"],
      capabilities: ["named_entity_recognition", "extraction", "relations"],
      inputs: ["text"],
    }),
  );
  await writeFile(
    join(staging, "source_revision.json"),
    JSON.stringify({ model: "fastino/gliner2.5-base-v1", revision }),
  );
  await rename(staging, destination);
  console.log(
    `Installed native GLiNER2.5: ${destination}. Restart inference to discover it.`,
  );
}
