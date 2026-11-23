import type {
  IndexPublicationFailure,
  IndexPublicationPhase,
  IndexPublicationState,
  IndexPublicationStatus,
  IndexPublicationToken,
  SQLDDLReceipt,
} from "../src/index.js";

const token: IndexPublicationToken = "recovery-handle";
const state: IndexPublicationState = "admission_unknown";
const phase: IndexPublicationPhase = "published";
const failure: IndexPublicationFailure = "duplicate_key";
const publication: IndexPublicationStatus = {
  index_type: "relational",
  index_name: "by_email",
  publication_token: token,
  table_id: "18446744073709551615",
  publication_id: "2".repeat(32),
  plan_digest: "3".repeat(64),
  metadata_incarnation: "1".repeat(32),
  state,
  phase,
  replication_pending: true,
};
const receipt: SQLDDLReceipt = {
  database: "default",
  namespace: "public",
  table: "items",
  table_id: publication.table_id,
  schema_version: 2,
  state: "admission_unknown",
  index_publication_token: token,
  fk_generation_publication_id: publication.publication_id,
};
void [publication, receipt, failure];
