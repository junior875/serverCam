"""
mqtt_client.py — Canal de controle MQTT (substitui WebSocket)

Tópicos:
  hivegrid/{MAC}/up    — câmera publica (register, ping, status, ack, snapshot_done, ...)
  hivegrid/{MAC}/down  — servidor publica (registered, init, pong, comandos)

{MAC} = uppercase com dois-pontos, ex: AA:BB:CC:DD:EE:FF
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

import aiomqtt

from db import get_device, upsert_device, update_device_last_seen
from models import ActiveConnection, DEFAULT_CONFIG, active_connections

logger = logging.getLogger("fluxcam.mqtt")

MQTT_BROKER    = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT      = int(os.environ.get("MQTT_PORT", 1883))
MQTT_USERNAME  = os.environ.get("MQTT_USERNAME") or None
MQTT_PASSWORD  = os.environ.get("MQTT_PASSWORD") or None
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "fluxcam-server")

# Singleton do cliente MQTT — preenchido quando conectado, None quando offline
_client: Optional[aiomqtt.Client] = None


# ─── Publicação ───────────────────────────────────────────────────────────────

async def send_command(mac: str, payload: dict) -> bool:
    """Publica JSON no tópico hivegrid/{mac}/down. Retorna True se enviado."""
    if _client is None:
        logger.warning("MQTT not connected — dropping command to %s: %s", mac, payload.get("type"))
        return False
    topic = f"hivegrid/{mac}/down"
    try:
        await _client.publish(topic, json.dumps(payload).encode())
        return True
    except Exception as exc:
        logger.warning("MQTT publish failed for %s: %s", mac, exc)
        return False


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _log(mac: str, msg: str, level: str = "info") -> None:
    ts = datetime.now(timezone.utc).isoformat()
    getattr(logger, level)("[%s] [%s] %s", ts, mac, msg)


def _clamp_score(score: float) -> int:
    return max(0, min(100, int(round(score))))


def _compute_lq_score(conn: ActiveConnection | None) -> int:
    if not conn:
        return 100
    stats = conn.link_stats or {}
    pkt_loss   = float(stats.get("pkt_loss",   0) or 0)
    avg_rtt    = float(stats.get("avg_rtt",    0) or 0)
    link_score = stats.get("link_score")
    base_score = float(link_score) if link_score is not None else 100.0
    penalty    = (pkt_loss * 2.0) + max(0.0, avg_rtt - 100.0) / 5.0
    return _clamp_score(base_score - penalty)


def _build_config(config: dict) -> dict:
    return {
        "servo_h":       config.get("servo_h",       90),
        "servo_v":       config.get("servo_v",       90),
        "flash_main":    config.get("flash_main",     0),
        "flash_monitor": config.get("flash_monitor",  0),
    }


# ─── Handlers por tipo de mensagem ────────────────────────────────────────────

async def _handle_register(mac: str, data: dict) -> None:
    firmware: str = data.get("firmware", "unknown")
    caps: list    = data.get("caps", [])
    _log(mac, f"register firmware={firmware} caps={caps}")

    device = await get_device(mac)
    if device:
        raw_cfg = device.get("config")
        config: dict = (
            raw_cfg if isinstance(raw_cfg, dict)
            else json.loads(raw_cfg) if raw_cfg
            else DEFAULT_CONFIG.copy()
        )
    else:
        config = DEFAULT_CONFIG.copy()

    await upsert_device(mac, firmware, caps, config)

    session_key = secrets.token_hex(16)
    conn = ActiveConnection(
        mac=mac,
        session_key=session_key,
        caps=caps,
        last_seen=time.time(),
        config=config,
        online=True,
    )
    active_connections[mac] = conn

    # 1. registered — câmera precisa disto para sair do modo silencioso
    await send_command(mac, {
        "type":        "registered",
        "session_key": session_key,
        "server_time": int(time.time()),
    })
    # 2. init — configura servos e flash
    await send_command(mac, {
        "type": "init",
        **_build_config(config),
    })
    _log(mac, f"registered session_key={session_key}")


async def _handle_ping(mac: str, data: dict) -> None:
    seq  = data.get("seq", 0)
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
        conn.online    = True
    await update_device_last_seen(mac)
    lq_score = _compute_lq_score(conn)
    _log(mac, f"ping seq={seq} -> pong lq_score={lq_score}")
    await send_command(mac, {"type": "pong", "seq": seq, "lq_score": lq_score})


async def _handle_status(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
        conn.online    = True
        conn.status    = data
        conn.link_stats.update({
            "pkt_loss":   data.get("pkt_loss"),
            "avg_rtt":    data.get("avg_rtt"),
            "link_score": data.get("link_score"),
            "rssi":       data.get("rssi"),
            "heap":       data.get("heap"),
        })
    await update_device_last_seen(mac)
    _log(mac, f"status uptime={data.get('uptime')} rssi={data.get('rssi')} "
              f"heap={data.get('heap')} link_score={data.get('link_score')} "
              f"pkt_loss={data.get('pkt_loss')} avg_rtt={data.get('avg_rtt')}")


async def _handle_ack(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
        conn.ack_count += 1
    _log(mac, f"ack seq={data.get('seq')} ref={data.get('ref')}")


async def _handle_snapshot_done(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen     = time.time()
        conn.last_image_seq = data.get("seq")
    _log(mac, f"snapshot_done seq={data.get('seq')} size={data.get('size')} "
              f"{data.get('width')}x{data.get('height')}")


async def _handle_burst_done(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
    total = data.get("total", data.get("count", "?"))
    _log(mac, f"burst_done burst_id={data.get('burst_id')} total={total}")


async def _handle_snapshot_error(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
    _log(mac, f"snapshot_error seq={data.get('seq')} reason={data.get('reason')}", level="warning")


async def _handle_error(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
    _log(mac, f"camera error code={data.get('code')} heap={data.get('heap')} "
              f"min_heap={data.get('min_heap')}", level="warning")


_HANDLERS = {
    "register":       _handle_register,
    "ping":           _handle_ping,
    "status":         _handle_status,
    "ack":            _handle_ack,
    "snapshot_done":  _handle_snapshot_done,
    "snapshot_error": _handle_snapshot_error,
    "burst_done":     _handle_burst_done,
    "error":          _handle_error,
}


async def _dispatch_message(mac: str, data: dict) -> None:
    msg_type = data.get("type", "")
    handler  = _HANDLERS.get(msg_type)
    if handler:
        await handler(mac, data)
    else:
        _log(mac, f"unknown type '{msg_type}'", level="warning")


# ─── Loop principal com reconexão automática ──────────────────────────────────

async def run_mqtt() -> None:
    global _client
    while True:
        try:
            logger.info("MQTT connecting to %s:%d ...", MQTT_BROKER, MQTT_PORT)
            async with aiomqtt.Client(
                hostname   = MQTT_BROKER,
                port       = MQTT_PORT,
                username   = MQTT_USERNAME,
                password   = MQTT_PASSWORD,
                identifier = MQTT_CLIENT_ID,
                keepalive  = 60,
            ) as client:
                _client = client
                await client.subscribe("hivegrid/+/up")
                logger.info("MQTT connected — subscribed to hivegrid/+/up")

                async for message in client.messages:
                    topic  = str(message.topic)
                    parts  = topic.split("/")
                    if len(parts) != 3 or parts[2] != "up":
                        continue
                    mac = parts[1].upper()
                    try:
                        data = json.loads(message.payload)
                    except json.JSONDecodeError:
                        logger.warning("MQTT invalid JSON from %s: %r", mac, message.payload[:120])
                        continue
                    try:
                        await _dispatch_message(mac, data)
                    except Exception as exc:
                        logger.exception("MQTT dispatch error for %s: %s", mac, exc)

        except aiomqtt.MqttError as exc:
            _client = None
            logger.warning("MQTT disconnected: %s — reconnecting in 5s", exc)
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            _client = None
            logger.info("MQTT task cancelled")
            raise
        except Exception as exc:
            _client = None
            logger.exception("MQTT unexpected error: %s — reconnecting in 5s", exc)
            await asyncio.sleep(5)
