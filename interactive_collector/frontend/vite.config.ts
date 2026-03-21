import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  base: "/",
  server: {
    proxy: {
      "/api": "http://127.0.0.1:5000",
      // Launcher must be served by Flask (redirect HTML), not SPA index.html.
      "/extension": "http://127.0.0.1:5000",
    },
  },
});
