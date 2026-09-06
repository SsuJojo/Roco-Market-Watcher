import json
from pathlib import Path

from app.main import app


def _server_options() -> tuple[str, int]:
    """Read the local launcher's bind address and port from config.json.

    Keep the secure loopback default when the optional server settings are
    missing or malformed. Deployments that need a public bind address should
    configure it explicitly (or pass options to uvicorn directly).
    """
    config_path = Path(__file__).resolve().with_name("config.json")
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        config = {}

    server = config.get("server") if isinstance(config, dict) else None
    if not isinstance(server, dict):
        server = {}

    host = server.get("host")
    if not isinstance(host, str) or not host.strip():
        host = "127.0.0.1"
    else:
        host = host.strip()

    port = server.get("port")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        port = 8000
    return host, port


if __name__ == "__main__":
    import uvicorn

    host, port = _server_options()
    uvicorn.run(app, host=host, port=port)
