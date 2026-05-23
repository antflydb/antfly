export interface AntfarmRuntimeConfig {
  basePath?: string;
  apiUrl?: string;
  termiteApiUrl?: string;
  authMode?: "local" | "external";
}

declare global {
  interface Window {
    __ANTFARM_CONFIG__?: AntfarmRuntimeConfig;
  }
}

export function getAntfarmRuntimeConfig(): AntfarmRuntimeConfig {
  if (typeof window === "undefined") {
    return {};
  }
  return window.__ANTFARM_CONFIG__ ?? {};
}

export function getAntfarmBasePath(): string {
  const configured = getAntfarmRuntimeConfig().basePath?.trim();
  if (!configured || configured === "/") {
    return "";
  }
  return configured.replace(/\/$/, "");
}

export function isExternalAuthMode(): boolean {
  return getAntfarmRuntimeConfig().authMode === "external";
}
