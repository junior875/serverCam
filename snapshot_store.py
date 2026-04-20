"""
Snapshot store — armazenamento em memória dos frames capturados.

Cada device mantém uma fila circular de até MAX_PER_DEVICE snapshots.
Burst sessions são agrupadas por burst_id para facilitar listagem/playback.
"""
from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

MAX_PER_DEVICE = 60  # máximo de frames mantidos por device (dobro de um burst típico)
SNAPSHOT_DIR = Path(os.environ.get("SNAPSHOT_DIR", "./snapshots"))


@dataclass
class Snapshot:
    mac: str
    seq: int                       # seq global por device (câmera controla)
    data: bytes
    content_type: str
    size: int
    timestamp: float               # unix timestamp da chegada no servidor
    burst_id: Optional[str]        # None se snapshot avulso
    width: Optional[int]
    height: Optional[int]
    quality: Optional[int]
    path: str


# mac → deque[Snapshot]
_store: dict[str, deque[Snapshot]] = {}

# mac → seq counter (usado quando a câmera não enviar seq)
_seq_counter: dict[str, int] = {}


def chip_id_from_mac(mac: str) -> str:
    compact = "".join(ch for ch in mac if ch.isalnum()).lower()
    if len(compact) < 8:
        raise ValueError(f"invalid MAC address: {mac}")
    return compact[-8:]


def next_seq(mac: str) -> int:
    if mac not in _store:
        _store[mac] = deque(maxlen=MAX_PER_DEVICE)
        _seq_counter[mac] = 0
    _seq_counter[mac] += 1
    return _seq_counter[mac]


def _snapshot_path(mac: str, burst_id: str | None, seq: int) -> Path:
    mac_safe = mac.replace(":", "-").lower()
    bucket = burst_id or "single"
    return SNAPSHOT_DIR / mac_safe / bucket / f"{seq:06d}.jpg"


def _write_snapshot_file(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


async def store(
    mac: str,
    data: bytes,
    content_type: str = "image/jpeg",
    seq: int | None = None,
    burst_id: str | None = None,
    width: int | None = None,
    height: int | None = None,
    quality: int | None = None,
) -> Snapshot:
    if mac not in _store:
        _store[mac] = deque(maxlen=MAX_PER_DEVICE)
        _seq_counter[mac] = 0

    if seq is None:
        seq = next_seq(mac)
    else:
        _seq_counter[mac] = max(_seq_counter.get(mac, 0), seq)

    path = _snapshot_path(mac, burst_id, seq)
    await asyncio.to_thread(_write_snapshot_file, path, data)

    snap = Snapshot(
        mac=mac,
        seq=seq,
        data=data,
        content_type=content_type,
        size=len(data),
        timestamp=time.time(),
        burst_id=burst_id,
        width=width,
        height=height,
        quality=quality,
        path=str(path),
    )
    _store[mac].append(snap)
    return snap


def get_latest(mac: str) -> Snapshot | None:
    q = _store.get(mac)
    return q[-1] if q else None


def get_by_seq(mac: str, seq: int) -> Snapshot | None:
    for snap in (_store.get(mac) or []):
        if snap.seq == seq:
            return snap
    return None


def get_burst(mac: str, burst_id: str) -> list[Snapshot]:
    return [s for s in (_store.get(mac) or []) if s.burst_id == burst_id]


def list_meta(mac: str) -> list[dict]:
    return [
        {
            "seq": s.seq,
            "size": s.size,
            "timestamp": s.timestamp,
            "burst_id": s.burst_id,
            "width": s.width,
            "height": s.height,
            "quality": s.quality,
            "path": s.path,
        }
        for s in (_store.get(mac) or [])
    ]


def device_stats(mac: str) -> dict:
    q = _store.get(mac)
    if not q:
        return {"count": 0}
    return {
        "count": len(q),
        "oldest_seq": q[0].seq,
        "latest_seq": q[-1].seq,
        "total_bytes": sum(s.size for s in q),
    }
