"""Event-driven order pipeline demo."""

import warnings

# Starlette's TestClient warns about its httpx dependency; it is only used by the
# in-process demo and tests.
warnings.filterwarnings("ignore", message=".*httpx.*")

__version__ = "1.0.0"
