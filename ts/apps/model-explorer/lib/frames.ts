import q40Json from "@/data/generated/frames/gemma4-decode-q4_0.json";
import q80Json from "@/data/generated/frames/gemma4-decode-q8_0-anchor.json";
import { FrameScenario } from "@/lib/schema";

export const frameQ40 = FrameScenario.parse(q40Json);
export const frameQ80Anchor = FrameScenario.parse(q80Json);
