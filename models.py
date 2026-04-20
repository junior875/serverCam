from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional

# flash_main / flash_monitor: inteiro 0–10 (0 = desligado, 10 = máximo)
DEFAULT_CONFIG: dict = {
    "servo_h": 90,
    "servo_v": 90,
    "flash_main": 0,
    "flash_monitor": 0,
}


@dataclass
class ActiveConnection:
    mac: str
    session_key: str
    caps: list
    last_seen: float
    config: dict
    online: bool = True
    status: Optional[dict] = field(default=None)
    link_stats: dict = field(default_factory=dict)
    last_image: Optional[str] = None
    last_image_seq: Optional[int] = None
    ack_count: int = 0


# mac -> ActiveConnection
active_connections: dict[str, ActiveConnection] = {}
