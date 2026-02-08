"""Entry point for running as python -m app."""

import os

from app.server import mcp

transport = os.environ.get("MCP_TRANSPORT", "stdio")
mcp.run(transport=transport)
