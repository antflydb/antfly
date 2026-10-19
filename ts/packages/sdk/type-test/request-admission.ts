import {
  AntflyClient,
  ClientCapacityError,
  InferenceClient,
  RequestAdmission,
  type RequestAdmissionOptions,
} from "../src/index.js";

const options: RequestAdmissionOptions = { maxConcurrent: 8, maxQueued: 16, maxWaitMs: 250 };
const shared = new RequestAdmission(options);
const db = new AntflyClient({ baseUrl: "http://localhost:8080", requestAdmission: shared });
const ai = new InferenceClient({ baseUrl: "http://localhost:8080", requestAdmission: shared });
new AntflyClient({ baseUrl: "http://localhost:8080", requestAdmission: options });
new InferenceClient({ baseUrl: "http://localhost:8080", requestAdmission: options });
const sent: false = new ClientCapacityError("queue_full").requestSent;
void sent;
const fetch: typeof globalThis.fetch = shared.fetch;
void fetch;
const active: number = shared.stats.active;
void active;
db.close();
ai.close();
shared.close();
