"""CLI entry point for uvx compatibility."""

from __future__ import annotations

from app.auth import BearerAuthMiddleware
from app.config import load_config
from app.server import mcp


def main() -> None:
    config = load_config()

    if config.transport == "stdio":
        mcp.run(transport="stdio")
        return

    if config.transport == "sse":
        import uvicorn

        if not config.auth_token:
            raise RuntimeError(
                "SSE transport reached run() without an auth token — load_config() bug"
            )
        wrapped = BearerAuthMiddleware(mcp.sse_app(), token=config.auth_token)
        uvicorn.run(
            wrapped,
            host=mcp.settings.host,
            port=mcp.settings.port,
            log_level=mcp.settings.log_level.lower(),
        )
        return

    raise ValueError(f"Unsupported MCP_TRANSPORT: {config.transport!r}")


if __name__ == "__main__":
    main()
