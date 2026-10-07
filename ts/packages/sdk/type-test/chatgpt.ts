import type { GeneratorConfig } from "../src/types.js";

const personal: GeneratorConfig = {
  provider: "chatgpt",
  connection_id: "one",
  model: "catalog-slug",
};
void personal;
// @ts-expect-error Personal plans require an owner-bound registration.
const missing: GeneratorConfig = { provider: "chatgpt", model: "catalog-slug" };
void missing;
const key: GeneratorConfig = {
  provider: "chatgpt",
  connection_id: "one",
  model: "catalog-slug",
  // @ts-expect-error API keys cannot be attached to a personal plan.
  api_key: "key",
};
void key;
// @ts-expect-error Personal plans do not accept temperature.
const temperature: GeneratorConfig = {
  provider: "chatgpt",
  connection_id: "one",
  model: "catalog-slug",
  temperature: 0.5,
};
void temperature;
