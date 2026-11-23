import type {
  RelationalIndexPredicate,
  RelationalUniqueConstraint,
  RelationalUniqueConstraintOrigin,
} from "../src/index.js";

const emptyMembership: RelationalIndexPredicate = { constant: false };
const fullMembership: RelationalIndexPredicate = { constant: true };
void emptyMembership;
void fullMembership;
// @ts-expect-error Membership truth is boolean, not a numeric comparison operand.
const invalidMembership: RelationalIndexPredicate = { constant: 0 };
void invalidMembership;

const origin: RelationalUniqueConstraintOrigin = "index";
const rule: RelationalUniqueConstraint = { name: "email_key", columns: ["email"], origin };
void rule;
const ordinary: RelationalUniqueConstraint = { name: "named_key", columns: ["id"] };
void ordinary;
// @ts-expect-error Display labels are not ownership identities.
const invalid: RelationalUniqueConstraintOrigin = "SQL UNIQUE INDEX";
void invalid;
