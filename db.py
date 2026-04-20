from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg

_pool: asyncpg.Pool | None = None


def _get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool not initialized — call init_db() first")
    return _pool


def _db_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    # Railway às vezes fornece postgres://, asyncpg exige postgresql://
    return url.replace("postgres://", "postgresql://", 1)


async def init_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(_db_url(), min_size=1, max_size=5)

    async with _get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS devices (
                mac        TEXT PRIMARY KEY,
                firmware   TEXT,
                caps       JSONB    NOT NULL DEFAULT '[]',
                config     JSONB    NOT NULL DEFAULT '{}',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)


async def get_device(mac: str) -> dict | None:
    async with _get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM devices WHERE mac = $1", mac
        )
        return dict(row) if row else None


async def upsert_device(
    mac: str, firmware: str, caps: list, config: dict
) -> None:
    async with _get_pool().acquire() as conn:
        existing = await conn.fetchval(
            "SELECT mac FROM devices WHERE mac = $1", mac
        )
        if existing:
            await conn.execute(
                "UPDATE devices SET firmware = $1, caps = $2, last_seen = NOW() WHERE mac = $3",
                firmware, json.dumps(caps), mac,
            )
        else:
            await conn.execute(
                """INSERT INTO devices (mac, firmware, caps, config)
                   VALUES ($1, $2, $3, $4)""",
                mac, firmware, json.dumps(caps), json.dumps(config),
            )


async def update_device_config(mac: str, config: dict) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE devices SET config = $1, last_seen = NOW() WHERE mac = $2",
            json.dumps(config), mac,
        )


async def update_device_last_seen(mac: str) -> None:
    async with _get_pool().acquire() as conn:
        await conn.execute(
            "UPDATE devices SET last_seen = NOW() WHERE mac = $1", mac
        )


async def get_all_devices() -> list[dict]:
    async with _get_pool().acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM devices ORDER BY last_seen DESC"
        )
        result = []
        for row in rows:
            d = dict(row)
            # asyncpg retorna JSONB como str quando coluna é TEXT-encoded
            if isinstance(d.get("caps"), str):
                d["caps"] = json.loads(d["caps"])
            if isinstance(d.get("config"), str):
                d["config"] = json.loads(d["config"])
            # converte timestamptz para ISO string
            for ts_col in ("created_at", "last_seen"):
                if d.get(ts_col) and not isinstance(d[ts_col], str):
                    d[ts_col] = d[ts_col].isoformat()
            result.append(d)
        return result


async def close_db() -> None:
    if _pool:
        await _pool.close()
