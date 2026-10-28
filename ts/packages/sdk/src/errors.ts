/** Structured HTTP failures. A failed/aborted transport does not prove a mutation was rolled back. */
export class AntflyHTTPError extends Error {
  constructor(
    message: string,
    public readonly status: number,
    public readonly body: unknown
  ) {
    super(message);
    this.name = "AntflyHTTPError";
  }
}

/** An unusable success response; mutation callers must reconcile before retrying. */
export class AntflyResponseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AntflyResponseError";
  }
}
