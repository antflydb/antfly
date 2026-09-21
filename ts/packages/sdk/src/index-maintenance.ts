import type { IndexMaintenanceRequest, IndexMaintenanceResponse } from "./types.js";

function decimal(value: unknown, positive: boolean): boolean {
  return (
    typeof value === "string" &&
    /^(0|[1-9][0-9]{0,19})$/.test(value) &&
    BigInt(value) <= 18446744073709551615n &&
    (!positive || value !== "0")
  );
}

function digest(value: unknown): boolean {
  return typeof value === "string" && /^[0-9a-f]{64}$/.test(value);
}

export function validateIndexMaintenanceRequest(request: IndexMaintenanceRequest): Set<string> {
  if (
    !request ||
    !decimal(request.table_id, true) ||
    !Number.isInteger(request.schema_version) ||
    request.schema_version < 0 ||
    request.schema_version > 4294967295 ||
    !Array.isArray(request.owners) ||
    request.owners.length < 1 ||
    request.owners.length > 128
  ) {
    throw new Error(
      "Index maintenance requires a valid table/schema identity and 1..128 owner proofs"
    );
  }
  const groups = new Set<string>();
  for (const owner of request.owners) {
    if (
      !owner ||
      !decimal(owner.group_id, true) ||
      !decimal(owner.generation, true) ||
      !decimal(owner.maintenance_epoch, false) ||
      !Number.isInteger(owner.slot) ||
      owner.slot < 0 ||
      owner.slot > 4294967295 ||
      !digest(owner.owner) ||
      !digest(owner.comparison) ||
      !digest(owner.progress_digest) ||
      groups.has(owner.group_id)
    ) {
      throw new Error("Index maintenance has an invalid or duplicate owner proof");
    }
    groups.add(owner.group_id);
  }
  return groups;
}

export function validateIndexMaintenanceResponse(
  value: unknown,
  groups: Set<string>
): IndexMaintenanceResponse {
  const response = value as IndexMaintenanceResponse | null;
  if (
    !response ||
    !Array.isArray(response.acknowledged_groups) ||
    response.acknowledged_groups.length !== groups.size
  ) {
    throw new Error("Incomplete index maintenance acknowledgement; resubmit identical proofs");
  }
  const remaining = new Set(groups);
  for (const group of response.acknowledged_groups) {
    if (typeof group !== "string" || !remaining.delete(group)) {
      throw new Error("Invalid index maintenance acknowledgement set; resubmit identical proofs");
    }
  }
  return response;
}
