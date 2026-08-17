import type { Metadata } from "next";
import { Shell } from "@/components/shell";
import { Providers } from "./providers";
import "./globals.css";

export const metadata: Metadata = {
  title: "@antfly/design-system — playground",
  description: "Component gallery for the Antfly design system.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body style={{ fontFamily: "var(--font-sans)" }}>
        <Providers>
          <Shell>{children}</Shell>
        </Providers>
      </body>
    </html>
  );
}
