"""Bearer-token authentication middleware for the SSE transport."""

from __future__ import annotations

import hmac
from typing import Awaitable, Callable

ASGIApp = Callable[[dict, Callable[[], Awaitable[dict]], Callable[[dict], Awaitable[None]]], Awaitable[None]]

_BEARER_PREFIX = "Bearer "
_UNAUTHORIZED_BODY = b'{"error":"unauthorized"}'


class BearerAuthMiddleware:
    """ASGI middleware that requires `Authorization: Bearer <token>` on HTTP requests.

    Implemented at raw ASGI level (not Starlette `BaseHTTPMiddleware`) to avoid
    interference with the SSE response streaming.
    """

    def __init__(self, app: ASGIApp, *, token: str) -> None:
        if not token:
            raise ValueError("BearerAuthMiddleware requires a non-empty token")
        self._app = app
        self._token = token

    async def __call__(self, scope: dict, receive, send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        provided = self._extract_token(scope)
        if provided is None or not hmac.compare_digest(provided, self._token):
            await self._reject(send)
            return

        await self._app(scope, receive, send)

    @staticmethod
    def _extract_token(scope: dict) -> str | None:
        for name, value in scope.get("headers", []):
            if name == b"authorization":
                decoded = value.decode("latin-1")
                if not decoded.startswith(_BEARER_PREFIX):
                    return None
                token = decoded[len(_BEARER_PREFIX):]
                return token or None
        return None

    @staticmethod
    async def _reject(send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"www-authenticate", b'Bearer realm="z2m-mcp"'),
                    (b"content-length", str(len(_UNAUTHORIZED_BODY)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": _UNAUTHORIZED_BODY})
