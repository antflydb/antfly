#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(dirname, "..");
const definitionsPath = path.join(appRoot, "src/data/command-definitions.json");
const indexPath = path.join(appRoot, "src/data/command-index.json");
const checkOnly = process.argv.includes("--check");

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function embeddingKey(command) {
  return `${command.label}\n${command.description}`;
}

function validateDefinitions(definitions) {
  const seen = new Set();
  const errors = [];

  for (const command of definitions) {
    if (seen.has(command.id)) {
      errors.push(`duplicate command id: ${command.id}`);
    }
    seen.add(command.id);

    if (command.type === "navigation" && !command.href) {
      errors.push(`${command.id} is navigation but has no href`);
    }
    if (command.type === "action" && !command.action) {
      errors.push(`${command.id} is an action but has no action`);
    }
    if (command.href && command.action) {
      errors.push(`${command.id} must not define both href and action`);
    }
  }

  if (errors.length > 0) {
    throw new Error(
      `Invalid command definitions:\n${errors.map((error) => `- ${error}`).join("\n")}`
    );
  }
}

function commandMetadata(command) {
  const metadata = {
    id: command.id,
    type: command.type,
    group: command.group,
    label: command.label,
    description: command.description,
    icon: command.icon,
  };

  if (command.href) metadata.href = command.href;
  if (command.action) metadata.action = command.action;
  if (command.product) metadata.product = command.product;
  if (command.adminOnly) metadata.adminOnly = command.adminOnly;

  return metadata;
}

function buildEmbeddingLookup(index) {
  const byText = new Map();

  for (const command of index.commands ?? []) {
    const embedding = command.embedding;
    if (!Array.isArray(embedding)) continue;

    byText.set(embeddingKey(command), embedding);
  }

  return byText;
}

function buildIndex(definitions, existingIndex) {
  const embeddingByText = buildEmbeddingLookup(existingIndex);
  const model = existingIndex.model ?? "all-MiniLM-L6-v2";
  const dimension = existingIndex.dimension ?? 384;
  const missingEmbeddings = [];

  const commands = definitions
    .filter((command) => command.semantic)
    .map((command) => {
      const embedding = embeddingByText.get(embeddingKey(command));
      if (!embedding) {
        missingEmbeddings.push(command.id);
        return null;
      }
      if (embedding.length !== dimension) {
        throw new Error(
          `${command.id} embedding dimension ${embedding.length} does not match index dimension ${dimension}`
        );
      }
      return {
        ...commandMetadata(command),
        embedding,
      };
    });

  if (missingEmbeddings.length > 0) {
    throw new Error(
      [
        "Missing embeddings for semantic commands:",
        ...missingEmbeddings.map((id) => `- ${id}`),
        "",
        "Run the embedding refresh flow for these commands, then rerun this script.",
        "This script intentionally reuses only embeddings whose label and description are unchanged.",
      ].join("\n")
    );
  }

  return {
    model,
    dimension,
    generatedFrom: "src/data/command-definitions.json",
    commands,
  };
}

function formatIndex(index) {
  return `${JSON.stringify(index, null, 2)}\n`;
}

function canonicalize(value) {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, canonicalize(value[key])])
    );
  }
  return value;
}

function sameIndex(currentIndex, nextIndex) {
  return JSON.stringify(canonicalize(currentIndex)) === JSON.stringify(canonicalize(nextIndex));
}

function formatWithBiome(filePath) {
  const result = spawnSync("biome", ["format", "--write", filePath], {
    cwd: appRoot,
    encoding: "utf8",
    stdio: "pipe",
  });

  if (result.status !== 0) {
    throw new Error(
      [
        "Generated command-index.json but failed to format it with Biome.",
        result.stderr?.trim(),
        result.stdout?.trim(),
      ]
        .filter(Boolean)
        .join("\n")
    );
  }
}

function main() {
  const definitions = readJson(definitionsPath);
  const existingIndex = readJson(indexPath);
  validateDefinitions(definitions);

  const nextIndex = buildIndex(definitions, existingIndex);

  if (sameIndex(existingIndex, nextIndex)) {
    console.log("command-index.json is up to date");
    return;
  }

  if (checkOnly) {
    throw new Error(
      "command-index.json is out of date; run pnpm --filter antfarm generate:commands"
    );
  }

  fs.writeFileSync(indexPath, formatIndex(nextIndex));
  formatWithBiome(indexPath);
  console.log(`Wrote ${path.relative(appRoot, indexPath)}`);
}

main();
