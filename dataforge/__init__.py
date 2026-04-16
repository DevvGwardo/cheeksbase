"""DataForge - Agent-first data platform."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dataforge")
except PackageNotFoundError:
    __version__ = "0.1.0.dev0"