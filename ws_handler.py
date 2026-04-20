from __future__ import annotations

import json
import logging
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from aiohttp import web

from db import get_device, upsert_device, update_device_last_seen
from models import ActiveConnection, DEFAULT_CONFIG, active_connections

logger = logging.getLogger("fluxcam.ws")


def _log(mac: str, msg: str, level: str = "info") -> None:
    ts = datetime.now(timezone.utc).isoformat()
    getattr(logger, level)("[%s] [%s] %s", ts, mac, msg)


def _build_config_payload(config: dict) -> dict:
    return {
        "servo_h": config.get("servo_h", 90),
        "servo_v": config.get("servo_v", 90),
        "flash_main": config.get("flash_main", 0),
        "flash_monitor": config.get("flash_monitor", 0),
    }


def _clamp_score(score: float) -> int:
    return max(0, min(100, int(round(score))))


def _compute_lq_score(conn: ActiveConnection | None) -> int:
    if not conn:
        return 100
    stats = conn.link_stats or {}
    pkt_loss = float(stats.get("pkt_loss", 0) or 0)
    avg_rtt = float(stats.get("avg_rtt", 0) or 0)
    link_score = stats.get("link_score")
    base_score = float(link_score) if link_score is not None else 100.0
    penalty = (pkt_loss * 2.0) + max(0.0, avg_rtt - 100.0) / 5.0
    return _clamp_score(base_score - penalty)


def _extract_seq(data: dict) -> int | None:
    seq = data.get("seq")
    if seq is None:
        return None
    try:
        return int(seq)
    except (TypeError, ValueError):
        return None


async def safe_send(conn: ActiveConnection, payload: dict) -> bool:
    """Envia JSON via WS com lock por conexao para evitar interleaving."""
    raw = json.dumps(payload)
    async with conn.send_lock:
        try:
            await conn.websocket.send_str(raw)
            return True
        except (ConnectionResetError, RuntimeError) as exc:
            logger.warning("WS send failed for active session: %s", exc)
        except Exception as exc:
            logger.exception("Unexpected WS send failure: %s", exc)
    return False


async def _txq_ack(conn: ActiveConnection, mac: str) -> None:
    """
    Envia ACK para a TxQueue da camera.

    A camera mantém uma fila confiavel (TxQueue) onde cada mensagem recebe
    um seq monotônico (1, 2, 3...). O servidor deve responder com
    {"type":"ack","seq":N} onde N é o seq esperado pela fila.

    O servidor usa seu proprio contador ack_seq por conexao, que incrementa
    1 a 1 para cada mensagem da TxQueue recebida (exceto ping, que usa pong).
    Ambos os contadores comecam em 1 na mesma conexao, garantindo sincronismo.
    """
    conn.ack_seq += 1
    await safe_send(conn, {"type": "ack", "seq": conn.ack_seq})
    _log(mac, f"txq_ack sent seq={conn.ack_seq}")


async def _handle_register(ws: web.WebSocketResponse, data: dict) -> Optional[str]:
    mac: Optional[str] = data.get("mac")
    if not mac:
        await ws.send_str(json.dumps({"type": "error", "message": "Missing MAC address"}))
        return None

    firmware: str = data.get("firmware", "unknown")
    caps: list = data.get("caps", [])
    _log(mac, f"register — firmware={firmware} caps={caps}")

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

    existing = active_connections.get(mac)
    if existing and existing.websocket is not ws:
        _log(mac, "closing previous connection before re-register")
        try:
            await existing.websocket.close()
        except Exception as exc:
            logger.warning("Failed to close previous connection for %s: %s", mac, exc)

    session_key = secrets.token_hex(16)
    conn = ActiveConnection(
        websocket=ws,
        session_key=session_key,
        caps=caps,
        last_seen=time.time(),
        config=config,
        ack_seq=0,  # reseta contador TxQueue a cada nova sessao
    )
    active_connections[mac] = conn

    # register NAO passa pela TxQueue — responder registered + init sem ack
    await safe_send(conn, {"type": "registered", "session_key": session_key})
    await safe_send(conn, {"type": "init", "config": _build_config_payload(config)})

    _log(mac, f"registration complete — session_key={session_key}")
    return mac


async def _handle_message(mac: str, data: dict) -> None:
    conn = active_connections.get(mac)
    if conn:
        conn.last_seen = time.time()
    await update_device_last_seen(mac)

    msg_type: str = data.get("type", "")
    ping_seq = _extract_seq(data)  # seq explicito da mensagem (ping/snapshot)

    # ── ping ──────────────────────────────────────────────────────────────────
    # ping passa pela TxQueue mas o firmware usa pong (nao ack) para confirmar.
    # pong deve ecoar o mesmo seq do ping para o LinkMonitor calcular RTT.
    if msg_type == "ping":
        lq_score = _compute_lq_score(conn)
        _log(mac, f"ping — seq={ping_seq} lq_score={lq_score}")
        if conn:
            await safe_send(conn, {"type": "pong", "seq": ping_seq, "lq_score": lq_score})

    # ── status ────────────────────────────────────────────────────────────────
    elif msg_type == "status":
        _log(mac, f"status uptime={data.get('uptime')} rssi={data.get('rssi')} "
                  f"heap={data.get('heap')} link_score={data.get('link_score')} "
                  f"pkt_loss={data.get('pkt_loss')} avg_rtt={data.get('avg_rtt')}")
        if conn:
            conn.status = data
            conn.link_stats.update({
                "pkt_loss": data.get("pkt_loss"),
                "avg_rtt": data.get("avg_rtt"),
                "link_score": data.get("link_score"),
                "rssi": data.get("rssi"),
                "heap": data.get("heap"),
            })
            await _txq_ack(conn, mac)

    # ── ack da camera (resposta a init/cmd/update_config/set_tx_power) ────────
    # A camera envia ack confirmando que recebeu um comando do servidor.
    # Esse ack passa pela TxQueue da camera e precisa de ack de volta.
    elif msg_type == "ack":
        ref = data.get("ref", "?")
        _log(mac, f"ack from camera — ref={ref} servo_h={data.get('servo_h')} "
                  f"servo_v={data.get('servo_v')} power={data.get('power')}")
        if conn:
            await _txq_ack(conn, mac)

    # ── snapshot_done ─────────────────────────────────────────────────────────
    elif msg_type == "snapshot_done":
        _log(mac,
             f"snapshot_done — photo_seq={ping_seq} size={data.get('size')} "
             f"{data.get('width')}x{data.get('height')}")
        if conn:
            await _txq_ack(conn, mac)

    # ── snapshot_ready (alias legado) ─────────────────────────────────────────
    elif msg_type == "snapshot_ready":
        _log(mac, "legacy snapshot_ready — treating as snapshot_done", level="warning")
        await _handle_message(mac, {**data, "type": "snapshot_done"})
        return  # ack ja enviado pelo handler acima

    # ── snapshot_error ────────────────────────────────────────────────────────
    elif msg_type == "snapshot_error":
        _log(mac, f"snapshot_error — photo_seq={ping_seq} reason={data.get('reason')}",
             level="warning")
        if conn:
            await _txq_ack(conn, mac)

    # ── burst_done ────────────────────────────────────────────────────────────
    elif msg_type == "burst_done":
        _log(mac, f"burst_done — burst_id={data.get('burst_id')} count={data.get('count')}")
        if conn:
            await _txq_ack(conn, mac)

    # ── error (LOW_HEAP etc.) ─────────────────────────────────────────────────
    elif msg_type == "error":
        code = data.get("code", "UNKNOWN")
        _log(mac, f"camera error — code={code} heap={data.get('heap')} "
                  f"min_heap={data.get('min_heap')} mem_live={data.get('mem_live')}",
             level="warning")
        if conn:
            await _txq_ack(conn, mac)

    # ── stream_started (placeholder) ─────────────────────────────────────────
    elif msg_type == "stream_started":
        _log(mac, f"stream_started — url={data.get('url')}")
        if conn:
            await _txq_ack(conn, mac)

    # ── telemetria de link (nao passa pela TxQueue, sem ack necessario) ───────
    elif msg_type == "perf":
        if conn:
            conn.link_stats.update({
                "link_score": data.get("score"),
                "pkt_loss": data.get("loss"),
                "avg_rtt": data.get("rtt"),
                "heap": data.get("heap"),
            })
        _log(mac,
             f"perf — score={data.get('score')} loss={data.get('loss')} "
             f"rtt={data.get('rtt')} heap={data.get('heap')}")

    elif msg_type == "link":
        if conn:
            conn.link_stats.update(data)
        _log(mac, f"link — {data}")

    else:
        _log(mac, f"unknown message type '{msg_type}'", level="warning")


async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    # heartbeat=15s: servidor envia PING de controle a cada 15s
    # receive_timeout=60s: fecha conexao se camera sumir
    ws = web.WebSocketResponse(protocols=("arduino",), heartbeat=15, receive_timeout=60)
    await ws.prepare(request)

    remote = request.remote
    logger.info("New connection from %s", remote)
    mac: Optional[str] = None

    try:
        async for msg in ws:
            try:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("Invalid JSON from %s: %r", remote, msg.data)
                        continue

                    msg_type = data.get("type")
                    if msg_type == "register":
                        mac = await _handle_register(ws, data)
                    elif mac:
                        await _handle_message(mac, data)
                    else:
                        logger.warning(
                            "Unregistered client %s sent type='%s'", remote, msg_type
                        )

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.warning("WS error from %s: %s", remote, ws.exception())

            except Exception as exc:
                logger.exception("Error handling message from %s: %s", remote, exc)

    finally:
        entry = active_connections.get(mac) if mac else None
        if entry is not None and entry.websocket is ws:
            del active_connections[mac]
            _log(mac, "disconnected — removed from active map")

    return ws
