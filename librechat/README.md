# LibreChat demo for the DRP Pipeline MCP

Runs [LibreChat] in Docker and points it at the DRP Pipeline orchestration
MCP server running on the host. Gives you a browser chat UI for driving the
pipeline by natural language.

[LibreChat]: https://www.librechat.ai/

## Architecture

```
  Browser ──▶ LibreChat (Docker, :3081) ──SSE──▶ DRP MCP server (host, :8765)
                │                                         │
                ├─ mongo (Docker)                          ├─ config.json
                └─ meilisearch (Docker)                    ├─ drp_pipeline.db
                                                           └─ python main.py ...
```

The MCP server stays on the host so it can use your project venv, read
`config.json`, and drive `python main.py <module>` via subprocess exactly
the way Claude Code and Claude Desktop do.

## Setup

1. **Install Docker Desktop** and make sure it is running.
2. **Create a `.env`:**
   ```bash
   cd librechat
   cp .env.example .env
   # edit .env and set ANTHROPIC_API_KEY
   ```
3. **Start the DRP MCP server in SSE mode** (in a separate terminal):
   ```bash
   cd ..
   .venv/bin/python mcp_server/server.py --transport sse --port 8765
   ```
4. **Bring up LibreChat:**
   ```bash
   docker compose up -d
   ```
5. **Open** http://localhost:3081 — register a local account, then start a
   new chat using the default "DRP Pipeline (Claude)" spec.

## Verify it worked

Ask the assistant:

> How's the pipeline looking?

It should call `get_pipeline_stats` and reply with the current counts. If
the tool call fails, check:

- Is the MCP server still running on port 8765? (`curl http://localhost:8765/sse`)
- Does `docker logs drp-librechat` show the MCP server connected at startup?

## Shut down

```bash
docker compose down            # stops containers, keeps data
docker compose down -v         # also deletes mongo/meili volumes
```

## Ports

| Port | Service |
|------|---------|
| 3081 | LibreChat UI (change via `LIBRECHAT_PORT` in `.env`) |
| 8765 | DRP Pipeline MCP SSE endpoint (host, not Docker) |

MongoDB and MeiliSearch are kept inside the Docker network — not exposed to
the host — to avoid clashing with any other local instances.
