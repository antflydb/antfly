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
const commandTypes = new Set(["navigation", "action"]);
const commandGroups = new Set(["navigation", "tools", "quickActions"]);
const commandActions = new Set(["toggle-theme"]);
const products = new Set(["antfly", "inference"]);

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function embeddingKey(command) {
  return `${command.label}\n${command.description}`;
}

function validateDefinitions(definitions) {
  const seen = new Set();
  const errors = [];

  if (!Array.isArray(definitions)) {
    throw new Error("Invalid command definitions: expected a JSON array");
  }

  for (const command of definitions) {
    if (typeof command.id !== "string" || command.id.length === 0) {
      errors.push("command is missing a non-empty id");
      continue;
    }
    if (seen.has(command.id)) {
      errors.push(`duplicate command id: ${command.id}`);
    }
    seen.add(command.id);

    if (!commandTypes.has(command.type)) {
      errors.push(`${command.id} has invalid type: ${command.type}`);
    }
    if (!commandGroups.has(command.group)) {
      errors.push(`${command.id} has invalid group: ${command.group}`);
    }
    if (typeof command.label !== "string" || command.label.length === 0) {
      errors.push(`${command.id} is missing a non-empty label`);
    }
    if (typeof command.description !== "string" || command.description.length === 0) {
      errors.push(`${command.id} is missing a non-empty description`);
    }
    if (typeof command.icon !== "string" || command.icon.length === 0) {
      errors.push(`${command.id} is missing a non-empty icon`);
    }
    if (command.product && !products.has(command.product)) {
      errors.push(`${command.id} has invalid product: ${command.product}`);
    }
    if (command.type === "navigation" && !command.product) {
      errors.push(`${command.id} is navigation but has no product`);
    }
    if (command.adminOnly !== undefined && typeof command.adminOnly !== "boolean") {
      errors.push(`${command.id} has invalid adminOnly`);
    }
    if (typeof command.semantic !== "boolean") {
      errors.push(`${command.id} must set semantic to true or false`);
    }
    if (command.type === "navigation" && !command.href) {
      errors.push(`${command.id} is navigation but has no href`);
    }
    if (command.href && typeof command.href !== "string") {
      errors.push(`${command.id} has invalid href`);
    }
    if (command.type === "action" && !command.action) {
      errors.push(`${command.id} is an action but has no action`);
    }
    if (command.action && typeof command.action !== "string") {
      errors.push(`${command.id} has invalid action`);
    }
    if (command.action && !commandActions.has(command.action)) {
      errors.push(`${command.id} has unsupported action: ${command.action}`);
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
  const result = spawnSync("pnpm", ["exec", "biome", "format", "--write", filePath], {
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
    if (!checkOnly) {
      formatWithBiome(indexPath);
    }
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
