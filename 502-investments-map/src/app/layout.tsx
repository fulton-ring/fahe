import type { Metadata } from "next";
import "../index.css";
import "maplibre-gl/dist/maplibre-gl.css";

export const metadata: Metadata = {
  title: "502 Investments Map",
  description: "FAHE Section 502 Investments Visualization",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
