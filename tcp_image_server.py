"""
tcp_image_server.py — Servidor TCP binário para recepção de JPEG (porta 9000)

Protocolo do frame (câmera → servidor):

  Offset  Tamanho  Campo
  ────────────────────────────────────────────────
  0       4        MAGIC: b'HG\\x01\\x00'
  4       18       MAC (ASCII null-padded, ex: "AA:BB:CC:DD:EE:FF\\x00")
  22      8        DEVICE_ID (últimos 8 hex do MAC, null-padded)
  30      32       FILENAME (null-padded, ex: "000001\\x00...")
  62      4        SEQ (uint32 big-endian)
  66      2        WIDTH (uint16 big-endian)
  68      2        HEIGHT (uint16 big-endian)
  70      1        QUALITY (uint8)
  71      1        TYPE: 0x00=snapshot, 0x01=burst
  72      4        JPEG_LEN (uint32 big-endian)
  76      JPEG_LEN JPEG DATA (binário cru)
  ────────────────────────────────────────────────
  Header total: 76 bytes fixos

Resposta do servidor → câmera:
  b'OK'  (sucesso)
  b'ER'  (erro — câmera reenvia até UPLOAD_MAX_RETRIES=3)
"""
from __future__ import annotations

import asyncio
import logging
import os
import struct

from models import active_connections
from snapshot_store import store

logger = logging.getLogger("fluxcam.tcp")

MAGIC       = b"HG\x01\x00"
HEADER_SIZE = 76
MAX_JPEG    = 5 * 1024 * 1024  # 5 MB — rejeita frames absurdos
TCP_IMAGE_PORT = int(os.environ.get("TCP_IMAGE_PORT", 9000))


async def _handle_image(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    peer = writer.get_extra_info("peername", ("?", 0))

    async def _err(reason: str) -> None:
        logger.warning("[TCP] %s — rejected from %s", reason, peer)
        try:
            writer.write(b"ER")
            await writer.drain()
        finally:
            writer.close()

    # 1. Lê header fixo de 76 bytes
    try:
        header = await asyncio.wait_for(reader.readexactly(HEADER_SIZE), timeout=10.0)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
        return await _err("header timeout/incomplete")

    # 2. Valida magic
    if header[:4] != MAGIC:
        return await _err(f"bad magic {header[:4]!r}")

    # 3. Desempacota campos
    try:
        mac       = header[4:22].rstrip(b"\x00").decode("ascii")
        device_id = header[22:30].rstrip(b"\x00").decode("ascii")
        filename  = header[30:62].rstrip(b"\x00").decode("ascii")
    except UnicodeDecodeError:
        return await _err("ascii decode error in header fields")

    seq, width, height, quality, img_type = struct.unpack_from(">IHHBB", header, 62)
    (jpeg_len,) = struct.unpack_from(">I", header, 72)

    if jpeg_len == 0 or jpeg_len > MAX_JPEG:
        return await _err(f"invalid jpeg_len={jpeg_len}")

    # 4. Lê payload JPEG
    try:
        jpeg_data = await asyncio.wait_for(reader.readexactly(jpeg_len), timeout=30.0)
    except (asyncio.TimeoutError, asyncio.IncompleteReadError):
        return await _err("jpeg payload timeout/incomplete")

    # 5. Valida SOI do JPEG
    if jpeg_data[:2] != b"\xff\xd8":
        return await _err(f"not a JPEG (SOI={jpeg_data[:2]!r})")

    # 6. Determina burst_id e seq de armazenamento
    #    filename numérico → seq avulso; string → burst_id
    burst_id: str | None = None
    store_seq: int | None = seq or None
    try:
        store_seq = int(filename) if filename else seq or None
    except ValueError:
        burst_id  = filename[:32] or None
        store_seq = seq or None

    # 7. Normaliza MAC (uppercase com dois-pontos)
    mac_norm = mac.upper()

    # 8. Persiste usando snapshot_store (memória + disco)
    try:
        snap = await store(
            mac_norm,
            jpeg_data,
            "image/jpeg",
            seq=store_seq,
            burst_id=burst_id,
            width=width if width else None,
            height=height if height else None,
            quality=quality if quality else None,
        )
    except Exception as exc:
        logger.exception("[TCP] store failed for %s: %s", mac_norm, exc)
        return await _err("store failed")

    # 9. Atualiza estado em memória do device
    conn = active_connections.get(mac_norm)
    if conn:
        conn.last_image     = snap.path
        conn.last_image_seq = snap.seq

    logger.info(
        "[TCP] OK mac=%s seq=%d %dx%d q=%d %d bytes type=%s -> %s",
        mac_norm, snap.seq, width, height, quality, jpeg_len,
        "burst" if img_type else "snap", snap.path,
    )

    # 10. Responde OK
    try:
        writer.write(b"OK")
        await writer.drain()
    finally:
        writer.close()


async def run_tcp_image_server() -> None:
    server = await asyncio.start_server(
        _handle_image,
        host="0.0.0.0",
        port=TCP_IMAGE_PORT,
    )
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    logger.info("[TCP] image server listening on %s", addrs)
    async with server:
        await server.serve_forever()
