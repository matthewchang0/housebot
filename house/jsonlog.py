from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .utils import to_iso_z


class JsonLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, event: str, **payload: Any) -> None:
        record = {"timestamp": to_iso_z(datetime.utcnow()), "event": event, **payload}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str) + "\n")
