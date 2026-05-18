// build.mjs — esbuild-based Chrome extension builder (replaces Plasmo)
import * as esbuild from "esbuild"
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "fs"
import { resolve, dirname } from "path"
import { fileURLToPath } from "url"

const __dirname = dirname(fileURLToPath(import.meta.url))
const OUT_DIR = resolve(__dirname, "build/chrome-mv3-prod")

mkdirSync(OUT_DIR, { recursive: true })

// ── Bundle content script ─────────────────────────────────────────────────────
await esbuild.build({
  entryPoints: ["contents/binance-overlay.tsx"],
  bundle: true,
  outfile: `${OUT_DIR}/content.js`,
  platform: "browser",
  target: ["chrome112"],
  format: "iife",
  jsx: "automatic",
  jsxImportSource: "react",
  define: {
    "process.env.NODE_ENV": '"production"',
  },
  alias: {
    "~components/CopilotPanel": resolve(__dirname, "src/components/CopilotPanel.tsx"),
    "~lib/backend": resolve(__dirname, "src/lib/backend.ts"),
    "~lib/orderbook": resolve(__dirname, "src/lib/orderbook.ts"),
    "~lib/types": resolve(__dirname, "src/lib/types.ts"),
  },
  minify: true,
  sourcemap: false,
  logLevel: "info",
})

// ── Copy icon ─────────────────────────────────────────────────────────────────
try {
  copyFileSync("assets/icon.png", `${OUT_DIR}/icon128.png`)
  console.log("✅ Icon copied")
} catch {
  console.warn("⚠ No icon found, skipping")
}

// ── Write manifest.json ───────────────────────────────────────────────────────
const manifest = {
  manifest_version: 3,
  name: "Binance AI Copilot",
  description: "AI overlay, order book analytics, and one-click trade actions for Binance",
  version: "0.2.0",
  icons: { "128": "icon128.png" },
  permissions: ["storage", "notifications", "nativeMessaging"],
  host_permissions: [
    "https://www.binance.com/*",
    "https://fapi.binance.com/*",
    "http://127.0.0.1:8000/*",
    "http://127.0.0.1:8020/*",
    "http://127.0.0.1:8022/*",
  ],
  content_scripts: [
    {
      matches: ["https://www.binance.com/*"],
      js: ["content.js"],
      run_at: "document_idle",
    },
  ],
}

writeFileSync(`${OUT_DIR}/manifest.json`, JSON.stringify(manifest, null, 2), "utf-8")
console.log("✅ manifest.json written")
console.log(`\n🟢 Build complete → ${OUT_DIR}`)
