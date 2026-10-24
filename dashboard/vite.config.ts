// @ts-ignore
import { defineConfig } from "vite";
// @ts-ignore
import react from "@vitejs/plugin-react";

// The dashboard calls the API at a relative /api path and the dev server forwards it, so the browser only ever
// talks to the dashboard's own origin. This works from any host (localhost, an EC2 public IP, ...) without CORS.
// Under docker compose the target is the server container; running `npm run dev` directly falls back to localhost.
// @ts-ignore
const apiProxyTarget = process.env.SUNBEAM_API_PROXY_TARGET ?? "http://localhost:8000";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api": {
        target: apiProxyTarget,
        changeOrigin: true,
        rewrite: (path: string) => path.replace(/^\/api/, "")
      }
    }
  }
});
