import { writeFile } from "node:fs/promises";
import { generateBrief } from "../server/governance-agent.ts";
import { readSnapshot } from "../server/governance-data.ts";
const { data, id } = await readSnapshot();
let trace: unknown;
const brief = await generateBrief(
  process.argv[2] || "When did HB 2017 take effect?",
  id,
  data.passages,
  [],
  process.argv.includes("--semantic"),
  (result) => {
    trace = result;
  },
);
await writeFile(
  "data/governance/cache/agent-probe.json",
  JSON.stringify({ brief, trace }, null, 2),
);
console.log(JSON.stringify(brief, null, 2));
