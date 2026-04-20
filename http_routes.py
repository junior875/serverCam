from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone

from aiohttp import web

from db import get_all_devices, update_device_config
from models import active_connections
from mqtt_client import send_command
from snapshot_store import (
    chip_id_from_mac,
    device_stats,
    get_burst,
    get_by_seq,
    get_latest,
    list_meta,
    next_seq,
    store,
)

logger = logging.getLogger("fluxcam.http")

ALLOWED_COMMAND_TYPES = {
    "cmd",
    "update_config",
    "take_snapshot",
    "start_burst",
    "stop_burst",
}


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    logger.info("[%s] %s", ts, msg)


# ─── Auth middleware ──────────────────────────────────────────────────────────

API_KEY = os.environ.get("API_KEY", "")


@web.middleware
async def auth_middleware(request: web.Request, handler):
    # Health check e uploads da câmera dispensam API key.
    if (
        request.path == "/health"
        or request.path.startswith("/snapshot/")
        or request.path == "/upload"
    ):
        return await handler(request)

    if API_KEY:
        key = (
            request.headers.get("X-API-Key")
            or request.query.get("api_key")
        )
        if key != API_KEY:
            return web.json_response(
                {"error": "Unauthorized — forneça X-API-Key no header ou ?api_key= na query"},
                status=401,
            )

    return await handler(request)


# ─── Helper de dispatch MQTT ─────────────────────────────────────────────────

async def _dispatch(mac: str, payload: dict) -> bool:
    """Publica comando via MQTT se o device estiver registrado."""
    if mac not in active_connections:
        return False
    return await send_command(mac, payload)


def _normalize_mac(mac: str | None) -> str | None:
    if not mac:
        return None
    return mac.upper()


def _resolve_mac_from_device_id(device_id: str | None) -> str | None:
    if not device_id:
        return None
    device_id = device_id.lower()
    for mac in active_connections:
        try:
            if chip_id_from_mac(mac) == device_id:
                return mac
        except ValueError:
            continue
    return None


def _snapshot_identity_from_filename(filename: str | None) -> tuple[int | None, str | None]:
    if not filename:
        return None, None
    try:
        return int(filename), None
    except ValueError:
        return None, filename[:32]


async def _store_snapshot_for_mac(
    mac: str,
    data: bytes,
    content_type: str,
    seq: int | None,
    burst_id: str | None,
    width: int | None,
    height: int | None,
    quality: int | None,
):
    if seq == 0:
        seq = None

    snap = await store(
        mac,
        data,
        content_type,
        seq=seq,
        burst_id=burst_id,
        width=width,
        height=height,
        quality=quality,
    )
    _log(f"snapshot stored mac={mac} seq={snap.seq} size={snap.size}B burst={burst_id}")
    return snap


# ─── Health check (sem auth) ──────────────────────────────────────────────────

async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "devices": len(active_connections)})


# ─── Endpoints gerais ─────────────────────────────────────────────────────────

async def devices_handler(request: web.Request) -> web.Response:
    rows = await get_all_devices()
    result = []
    for row in rows:
        mac  = row["mac"]
        conn = active_connections.get(mac)
        snap_stats = device_stats(mac)
        caps   = row.get("caps")   or []
        config = row.get("config") or {}
        result.append({
            "mac":       mac,
            "firmware":  row["firmware"],
            "caps":      caps   if isinstance(caps, list)   else json.loads(caps),
            "config":    config if isinstance(config, dict) else json.loads(config),
            "created_at": row["created_at"],
            "last_seen": conn.last_seen if conn else row["last_seen"],
            "status":    "online" if (conn and conn.online) else "offline",
            "transport": "mqtt+tcp",
            "stats": {
                "heap":        conn.link_stats.get("heap")        if conn else None,
                "link_score":  conn.link_stats.get("link_score")  if conn else None,
                "rssi":        conn.link_stats.get("rssi")        if conn else None,
                "pkt_loss":    conn.link_stats.get("pkt_loss")    if conn else None,
                "avg_rtt":     conn.link_stats.get("avg_rtt")     if conn else None,
            },
            "last_image":     conn.last_image     if conn else None,
            "last_image_seq": conn.last_image_seq if conn else None,
            "snapshots":      snap_stats,
        })
    return web.json_response(result)


async def simulate_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    _log(f"POST /simulate/{mac} body={body}")
    sent = await _dispatch(mac, {**body, "type": "cmd"})
    if sent:
        return web.json_response({"status": "sent", "mac": mac})
    return web.json_response({"error": "Camera offline", "mac": mac}, status=404)


async def command_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "Invalid JSON body"}, status=400)

    cmd_type = body.get("type")
    if cmd_type not in ALLOWED_COMMAND_TYPES:
        return web.json_response(
            {"error": f"Invalid command type '{cmd_type}'", "allowed": sorted(ALLOWED_COMMAND_TYPES)},
            status=400,
        )

    _log(f"POST /command/{mac} type={cmd_type}")

    if cmd_type == "update_config":
        incoming_cfg = body.get("config", {})
        conn = active_connections.get(mac)
        if conn and incoming_cfg:
            conn.config = {**conn.config, **incoming_cfg}
            await update_device_config(mac, conn.config)

    if cmd_type == "take_snapshot" and "seq" not in body:
        body["seq"] = next_seq(mac)

    if cmd_type == "start_burst":
        body.setdefault("burst_id", uuid.uuid4().hex[:12])

    sent = await _dispatch(mac, dict(body))
    if sent:
        resp = {"status": "sent", "mac": mac, "type": cmd_type}
        if cmd_type == "start_burst":
            resp["burst_id"] = body["burst_id"]
        return web.json_response(resp)
    return web.json_response({"error": "Camera offline", "mac": mac}, status=404)


# ─── Endpoints de snapshot (upload HTTP — modo WiFi / legado) ─────────────────

async def snapshot_upload_handler(request: web.Request) -> web.Response:
    """
    POST /snapshot/{mac}
    Upload legado. Autenticação via X-Session-Key.
    """
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)

    session_key = request.headers.get("X-Session-Key", "")
    conn = active_connections.get(mac)
    if not conn or conn.session_key != session_key:
        logger.warning("snapshot upload rejected — mac=%s bad session_key", mac)
        return web.json_response({"error": "Invalid session_key"}, status=401)

    data = await request.read()
    if not data:
        return web.json_response({"error": "Empty body"}, status=400)

    seq      = int(request.headers.get("X-Seq", "0"))
    burst_id = request.headers.get("X-Burst-Id") or None
    width    = _int_header(request, "X-Width")
    height   = _int_header(request, "X-Height")
    quality  = _int_header(request, "X-Quality")
    ctype    = request.content_type or "image/jpeg"

    snap = await _store_snapshot_for_mac(mac, data, ctype, seq=seq,
                                         burst_id=burst_id, width=width,
                                         height=height, quality=quality)
    if conn:
        conn.last_image     = snap.path
        conn.last_image_seq = snap.seq

    return web.json_response(
        {"ok": True, "seq": snap.seq, "size": snap.size},
        headers={"X-Seq": str(snap.seq), "X-Size": str(snap.size)},
    )


async def upload_handler(request: web.Request) -> web.Response:
    """
    POST /upload?device_id=<chip_id>&filename=<burst_id_or_seq>
    Endpoint canônico para upload HTTP (modo WiFi).
    """
    device_id = request.query.get("device_id")
    filename  = request.query.get("filename")
    mac = (
        _normalize_mac(request.headers.get("X-Device-Mac"))
        or _resolve_mac_from_device_id(device_id)
    )
    if not mac:
        return web.json_response({"error": "Unknown device"}, status=404)

    session_key = request.headers.get("X-Session-Key", "")
    conn = active_connections.get(mac)
    if not conn or conn.session_key != session_key:
        logger.warning("upload rejected — mac=%s bad session_key", mac)
        return web.json_response({"error": "Invalid session_key"}, status=401)

    if device_id:
        try:
            expected = chip_id_from_mac(mac)
        except ValueError:
            return web.json_response({"error": "Invalid device MAC"}, status=400)
        if device_id.lower() != expected:
            return web.json_response({"error": "device_id does not match MAC"}, status=400)

    data = await request.read()
    if not data:
        return web.json_response({"error": "Empty body"}, status=400)

    seq          = _int_header(request, "X-Seq")
    width        = _int_header(request, "X-Width")
    height       = _int_header(request, "X-Height")
    quality      = _int_header(request, "X-Quality")
    file_seq, file_burst_id = _snapshot_identity_from_filename(filename)
    seq      = seq      if seq      is not None else file_seq
    burst_id = request.headers.get("X-Burst-Id") or file_burst_id
    ctype    = request.content_type or "application/octet-stream"

    snap = await _store_snapshot_for_mac(mac, data, ctype, seq=seq,
                                         burst_id=burst_id, width=width,
                                         height=height, quality=quality)
    if conn:
        conn.last_image     = snap.path
        conn.last_image_seq = snap.seq

    return web.json_response({"ok": True, "seq": snap.seq, "size": snap.size})


# ─── Endpoints de leitura de snapshots ───────────────────────────────────────

async def snapshot_latest_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    snap = get_latest(mac)
    if not snap:
        return web.json_response({"error": "No snapshot available"}, status=404)
    return web.Response(
        body=snap.data,
        content_type=snap.content_type,
        headers={
            "X-Seq":       str(snap.seq),
            "X-Size":      str(snap.size),
            "X-Timestamp": str(snap.timestamp),
            "X-Burst-Id":  snap.burst_id or "",
        },
    )


async def snapshot_by_seq_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    try:
        seq = int(request.match_info["seq"])
    except ValueError:
        return web.json_response({"error": "seq must be integer"}, status=400)
    snap = get_by_seq(mac, seq)
    if not snap:
        return web.json_response({"error": f"Snapshot seq={seq} not found"}, status=404)
    return web.Response(
        body=snap.data,
        content_type=snap.content_type,
        headers={"X-Seq": str(snap.seq), "X-Size": str(snap.size)},
    )


async def snapshot_list_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    return web.json_response({
        "mac":    mac,
        "stats":  device_stats(mac),
        "frames": list_meta(mac),
    })


async def snapshot_burst_handler(request: web.Request) -> web.Response:
    mac = _normalize_mac(request.match_info["mac"])
    if not mac:
        return web.json_response({"error": "Missing MAC"}, status=400)
    burst_id = request.match_info["burst_id"]
    frames   = get_burst(mac, burst_id)
    return web.json_response({
        "mac":      mac,
        "burst_id": burst_id,
        "count":    len(frames),
        "frames": [
            {
                "seq":       s.seq,
                "size":      s.size,
                "timestamp": s.timestamp,
                "url":       f"/snapshot/{mac}/{s.seq}",
            }
            for s in frames
        ],
    })


# ─── Utilidades ───────────────────────────────────────────────────────────────

def _int_header(request: web.Request, name: str) -> int | None:
    val = request.headers.get(name)
    try:
        return int(val) if val else None
    except ValueError:
        return None


# ─── App factory ──────────────────────────────────────────────────────────────

def build_app() -> web.Application:
    app = web.Application(
        middlewares=[auth_middleware],
        client_max_size=10 * 1024 * 1024,
    )

    # Health (sem auth)
    app.router.add_get("/health", health_handler)

    # Devices
    app.router.add_get("/devices", devices_handler)

    # Comandos
    app.router.add_post("/simulate/{mac}", simulate_handler)
    app.router.add_post("/command/{mac}",  command_handler)

    # Uploads da câmera (HTTP — modo WiFi / legado)
    app.router.add_post("/upload",         upload_handler)
    app.router.add_post("/snapshot/{mac}", snapshot_upload_handler)

    # Leitura de snapshots
    app.router.add_get("/snapshot/{mac}/latest",                 snapshot_latest_handler)
    app.router.add_get("/snapshot/{mac}/list",                   snapshot_list_handler)
    app.router.add_get("/snapshot/{mac}/{seq}",                  snapshot_by_seq_handler)
    app.router.add_get("/snapshot/{mac}/burst/{burst_id}/list",  snapshot_burst_handler)

    return app
