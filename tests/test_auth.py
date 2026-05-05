"""Tests for the bearer-token ASGI middleware."""

from __future__ import annotations

import hmac
from unittest.mock import patch

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from app.auth import BearerAuthMiddleware


TOKEN = "a" * 64


def _build_client(token: str = TOKEN) -> TestClient:
    async def echo(request):
        return PlainTextResponse("ok")

    inner = Starlette(routes=[Route("/anything", echo)])
    wrapped = BearerAuthMiddleware(inner, token=token)
    return TestClient(wrapped)


class TestBearerAuthMiddleware:
    def test_valid_token_passes(self) -> None:
        client = _build_client()
        resp = client.get("/anything", headers={"Authorization": f"Bearer {TOKEN}"})
        assert resp.status_code == 200
        assert resp.text == "ok"

    def test_missing_authorization_returns_401(self) -> None:
        client = _build_client()
        resp = client.get("/anything")
        assert resp.status_code == 401
        assert resp.headers.get("www-authenticate", "").lower().startswith("bearer")

    def test_wrong_scheme_returns_401(self) -> None:
        client = _build_client()
        resp = client.get("/anything", headers={"Authorization": f"Basic {TOKEN}"})
        assert resp.status_code == 401

    def test_wrong_token_returns_401(self) -> None:
        client = _build_client()
        resp = client.get("/anything", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    def test_empty_bearer_returns_401(self) -> None:
        client = _build_client()
        resp = client.get("/anything", headers={"Authorization": "Bearer "})
        assert resp.status_code == 401

    def test_trailing_whitespace_rejected(self) -> None:
        # Whitespace must be part of the compared bytes — no normalisation
        # before constant-time compare.
        client = _build_client()
        resp = client.get("/anything", headers={"Authorization": f"Bearer {TOKEN} "})
        assert resp.status_code == 401

    def test_constant_time_compare_used(self) -> None:
        client = _build_client()
        with patch("app.auth.hmac.compare_digest", wraps=hmac.compare_digest) as spy:
            resp = client.get("/anything", headers={"Authorization": f"Bearer {TOKEN}"})
        assert resp.status_code == 200
        assert spy.called

    async def test_lifespan_scope_passes_through(self) -> None:
        called = {"hit": False}

        async def inner(scope, receive, send):
            called["hit"] = True

        mw = BearerAuthMiddleware(inner, token=TOKEN)

        async def receive():
            return {"type": "lifespan.startup"}

        async def send(_msg):
            pass

        await mw({"type": "lifespan"}, receive, send)
        assert called["hit"] is True

    async def test_websocket_scope_passes_through(self) -> None:
        called = {"hit": False}

        async def inner(scope, receive, send):
            called["hit"] = True

        mw = BearerAuthMiddleware(inner, token=TOKEN)

        async def receive():
            return {}

        async def send(_msg):
            pass

        await mw({"type": "websocket", "headers": []}, receive, send)
        assert called["hit"] is True
