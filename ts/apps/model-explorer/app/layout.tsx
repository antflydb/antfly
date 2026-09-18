import type { Metadata } from "next";
import manifest from "@/data/generated/manifest.json";
import { SiteNav } from "@/components/site-nav";
import { Providers } from "./providers";
import "./globals.css";

export const metadata: Metadata = {
  title: { default: "Antfly Model Explorer", template: "%s · Antfly Model Explorer" },
  description:
    "How data flows through Antfly's Zig inference runtime — Gemma4 E2B/E4B, GLiNER2, GLiNER2.5, Qwen3 Embedding, and Qwen3-VL, from tokenization to Metal kernels.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body style={{ fontFamily: "var(--font-sans)" }}>
        <Providers>
          <a href="#main-content" className="skip-link">Skip to content</a>
          <SiteNav />
          <main id="main-content" tabIndex={-1} className="min-h-screen">{children}</main>
          <footer className="border-t px-4 py-6 text-center text-xs text-muted-foreground">
            Selected model walkthroughs · source snapshot{" "}
            <a className="underline" href={`${manifest.permalinkBase}/${manifest.gitCommit}`} target="_blank" rel="noreferrer">{manifest.gitCommit.slice(0, 10)}</a>
            {" · "}<a className="underline" href={`${manifest.permalinkBase}/${manifest.gitCommit}/zig/pkg/inference/MODEL_COMPATIBILITY.md`} target="_blank" rel="noreferrer">Model compatibility and execution policy</a>
          </footer>
        </Providers>
      </body>
    </html>
  );
}
