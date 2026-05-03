import type { Metadata } from "next";
import { Roboto_Mono } from "next/font/google";
import { Shell } from "@/components/shell";
import { Providers } from "./providers";
import "./globals.css";

/**
 * Roboto Mono — the "technical voice" font used by `MonoLabel`.
 * Exposed via CSS var so the library's `--font-mono` stack resolves it.
 */
const robotoMono = Roboto_Mono({
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  variable: "--font-roboto-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: "@antfly/design-system — playground",
  description: "Component gallery for the Antfly design system.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning className={robotoMono.variable}>
      <body style={{ fontFamily: "var(--font-sans)" }}>
        <Providers>
          <Shell>{children}</Shell>
        </Providers>
      </body>
    </html>
  );
}
