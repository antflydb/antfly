import type { RelationalUniqueConstraint, RelationalUniqueConstraintOrigin } from "../src/index.js";

const origin: RelationalUniqueConstraintOrigin = "index";
const rule: RelationalUniqueConstraint = { name: "email_key", columns: ["email"], origin };
void rule;
const ordinary: RelationalUniqueConstraint = { name: "named_key", columns: ["id"] };
void ordinary;
// @ts-expect-error Display labels are not ownership identities.
const invalid: RelationalUniqueConstraintOrigin = "SQL UNIQUE INDEX";
void invalid;
