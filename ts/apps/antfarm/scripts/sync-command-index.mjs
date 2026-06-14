#!/usr/bin/env node
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
    throw new Error(`Invalid command definitions:\n${errors.map((error) => `- ${error}`).join("\n")}`);
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

function formatArray(values, indent) {
  const chunks = [];
  for (let i = 0; i < values.length; i += 6) {
    chunks.push(`${indent}${values.slice(i, i + 6).join(", ")}`);
  }
  return `[\n${chunks.join(",\n")}\n${indent.slice(0, -2)}]`;
}

function formatCommand(command) {
  const lines = [
    `    {`,
    `      "id": ${JSON.stringify(command.id)},`,
    `      "type": ${JSON.stringify(command.type)},`,
    `      "group": ${JSON.stringify(command.group)},`,
    `      "label": ${JSON.stringify(command.label)},`,
    `      "description": ${JSON.stringify(command.description)},`,
  ];

  if (command.href) lines.push(`      "href": ${JSON.stringify(command.href)},`);
  if (command.action) lines.push(`      "action": ${JSON.stringify(command.action)},`);
  lines.push(`      "icon": ${JSON.stringify(command.icon)},`);
  if (command.product) lines.push(`      "product": ${JSON.stringify(command.product)},`);
  if (command.adminOnly) lines.push(`      "adminOnly": true,`);
  lines.push(`      "embedding": ${formatArray(command.embedding, "        ")}`);
  lines.push(`    }`);
  return lines.join("\n");
}

function formatIndex(index) {
  return [
    `{`,
    `  "model": ${JSON.stringify(index.model)},`,
    `  "dimension": ${index.dimension},`,
    `  "generatedFrom": ${JSON.stringify(index.generatedFrom)},`,
    `  "commands": [`,
    index.commands.map(formatCommand).join(",\n"),
    `  ]`,
    `}`,
    ``,
  ].join("\n");
}

function main() {
  const definitions = readJson(definitionsPath);
  const existingIndex = readJson(indexPath);
  validateDefinitions(definitions);

  const nextIndex = buildIndex(definitions, existingIndex);
  const nextText = formatIndex(nextIndex);
  const currentText = fs.readFileSync(indexPath, "utf8");

  if (currentText === nextText) {
    console.log("command-index.json is up to date");
    return;
  }

  if (checkOnly) {
    throw new Error("command-index.json is out of date; run pnpm --filter antfarm generate:commands");
  }

  fs.writeFileSync(indexPath, nextText);
  console.log(`Wrote ${path.relative(appRoot, indexPath)}`);
}

main();
