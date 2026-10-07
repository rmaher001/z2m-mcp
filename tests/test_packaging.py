"""The Docker image installs from pyproject.toml, not uv.lock, so every runtime
dependency must stay on the major version it was tested on, or a new major
release lands at build time. mcp 2.x removed mcp.server.fastmcp and
crash-looped the container on 2026-10-06."""

import tomllib
from pathlib import Path

from packaging.requirements import Requirement

PYPROJECT = Path(__file__).resolve().parent.parent / "pyproject.toml"


def _escapes_tested_major(dep: str) -> bool:
    req = Requirement(dep)
    if req.marker is not None or req.url is not None:
        return True  # a platform-only bound or a URL pin is not a bound everywhere
    floors = [s.version for s in req.specifier if s.operator == ">="]
    if not floors:
        return True
    next_major = f"{int(floors[0].split('.')[0]) + 1}.0"
    return req.specifier.contains(next_major, prereleases=True)


def test_every_runtime_dependency_stays_below_the_next_major():
    deps = tomllib.loads(PYPROJECT.read_text())["project"]["dependencies"]
    assert [d for d in deps if _escapes_tested_major(d)] == []


def test_the_check_catches_loose_bounds():
    assert _escapes_tested_major("mcp[cli]>=1.4.1")
    assert _escapes_tested_major("mcp[cli]>=1.4.1,<3")
    assert _escapes_tested_major("mcp[cli]>=1.4.1,<=99")
    assert _escapes_tested_major("mcp[cli]>=1.4.1,<2; sys_platform == 'win32'")
    assert not _escapes_tested_major("mcp[cli]>=1.4.1,<2")
    assert not _escapes_tested_major("aiomqtt>=2.5.0,<3")
