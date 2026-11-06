/** Structured HTTP failures. A failed/aborted transport does not prove a mutation was rolled back. */
export class AntflyHTTPError extends Error {
  /** Server retry delay. This alone never authorizes replaying a mutation. */
  readonly retryAfterSeconds: number | undefined;
  /** An admitted mutation may have applied; reconcile its state before retrying. */
  readonly mutationOutcome: "unknown" | undefined;

  constructor(
    message: string,
    public readonly status: number,
    public readonly body: unknown,
    headers?: Headers
  ) {
    super(message);
    this.name = "AntflyHTTPError";
    const delay = headers?.get("Retry-After")?.trim();
    const seconds = delay && /^\d+$/.test(delay) ? Number(delay) : undefined;
    this.retryAfterSeconds =
      seconds !== undefined && Number.isSafeInteger(seconds) && seconds >= 0 ? seconds : undefined;
    this.mutationOutcome =
      headers?.get("X-Antfly-Raft-Mutation-Outcome")?.trim().toLowerCase() === "unknown-v1"
        ? "unknown"
        : undefined;
  }
}

/** An unusable success response; mutation callers must reconcile before retrying. */
export class AntflyResponseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AntflyResponseError";
  }
}
