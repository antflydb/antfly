import { z } from "zod";

const optionalNumber = z.number().finite().positive().optional();
export const deviceProfileSchema = z.object({
  name: z.string().trim().max(120),
  chip: z.string().trim().max(120),
  cpuCores: optionalNumber,
  memoryGB: optionalNumber,
  bandwidthGBs: optionalNumber,
});
export const modelProfileSchema = z.object({
  quantization: z.string().trim().max(80),
  qualityPercent: z.number().finite().min(0).max(100).optional(),
  decodeTokensPerSecond: optionalNumber,
  prefillTokensPerSecond: optionalNumber,
  peakMemoryGB: optionalNumber,
  contextTokens: optionalNumber,
  notes: z.string().trim().max(1000),
});
export const runtimeProfileSchema = z.object({
  version: z.literal(1),
  device: deviceProfileSchema,
  models: z.record(z.string(), modelProfileSchema),
});
export type DeviceProfile = z.infer<typeof deviceProfileSchema>;
export type ModelProfile = z.infer<typeof modelProfileSchema>;
export type RuntimeProfile = z.infer<typeof runtimeProfileSchema>;
export const emptyDevice: DeviceProfile = { name: "", chip: "" };
export const emptyModelProfile: ModelProfile = { quantization: "", notes: "" };

export function readRuntimeProfile(raw: string | null): RuntimeProfile {
  try {
    return runtimeProfileSchema.parse(JSON.parse(raw ?? ""));
  } catch {
    return { version: 1, device: { ...emptyDevice }, models: {} };
  }
}

export interface ProfileAxis {
  label: string;
  value: string;
  score: number | undefined;
  scale: string;
}

export function profileAxes(device: DeviceProfile, profile: ModelProfile): ProfileAxis[] {
  const normalized = (value: number | undefined, target: number) =>
    value === undefined ? undefined : Math.min(1, Math.max(0, value / target));
  return [
    {
      label: "Quality",
      value: profile.qualityPercent === undefined ? "Not recorded" : `${profile.qualityPercent}%`,
      score: normalized(profile.qualityPercent, 100),
      scale: "Evaluation score · 100%",
    },
    {
      label: "Decode",
      value:
        profile.decodeTokensPerSecond === undefined
          ? "Not recorded"
          : `${profile.decodeTokensPerSecond} tok/s`,
      score: normalized(profile.decodeTokensPerSecond, 100),
      scale: "100 tok/s",
    },
    {
      label: "Context",
      value:
        profile.contextTokens === undefined
          ? "Not recorded"
          : `${profile.contextTokens.toLocaleString()} tokens`,
      score: normalized(profile.contextTokens, 131072),
      scale: "131,072 tokens",
    },
    {
      label: "Memory headroom",
      value:
        profile.peakMemoryGB === undefined ? "Not recorded" : `${profile.peakMemoryGB} GB peak`,
      score:
        profile.peakMemoryGB !== undefined && device.memoryGB !== undefined
          ? Math.max(0, 1 - profile.peakMemoryGB / device.memoryGB)
          : undefined,
      scale: "Free fraction of configured RAM · not an admission check",
    },
    {
      label: "Prefill",
      value:
        profile.prefillTokensPerSecond === undefined
          ? "Not recorded"
          : `${profile.prefillTokensPerSecond} tok/s`,
      score: normalized(profile.prefillTokensPerSecond, 1000),
      scale: "1,000 tok/s",
    },
  ];
}
