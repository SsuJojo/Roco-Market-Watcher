import subprocess
import tempfile
from pathlib import Path


def send_openclaw_message(command: str, message: str) -> int:
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as file:
        file.write(message)
        temp_path = Path(file.name)

    try:
        rendered = command.replace("{{message_file}}", str(temp_path))
        return subprocess.call(rendered, shell=True)
    finally:
        temp_path.unlink(missing_ok=True)
