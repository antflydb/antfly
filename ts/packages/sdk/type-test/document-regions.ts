import type { components } from "../src/public-api.js";

const nativeRegion: components["schemas"]["InferenceTextRegion"] = {
  text: "Vertical native text",
  origin: "native",
  bbox: null,
};
const positionedRegion: components["schemas"]["InferenceTextRegion"] = {
  text: "Transformed native text",
  origin: "native",
  bbox: [92, 100, 102, 167],
};
void nativeRegion;
void positionedRegion;
