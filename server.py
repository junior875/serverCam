"""
FluxCam Server
==============
Servidor único (HTTP API + MQTT client + TCP image server).

Canais:
  MQTT  hivegrid/+/up   — câmera → servidor (controle e telemetria)
  MQTT  hivegrid/+/down — servidor → câmera (comandos)
  TCP   :9000           — câmera → servidor (upload JPEG binário)
  HTTP  :8080           — API REST (dashboard, comandos, leitura de fotos)

Rotas HTTP:
  GET  /devices           — lista dispositivos (requer X-API-Key)
  POST /simulate/{mac}    — envia cmd livre      (requer X-API-Key)
  POST /command/{mac}     — envia comando tipado  (requer X-API-Key)
  POST /upload            — upload HTTP JPEG (modo WiFi / legado)
  POST /snapshot/{mac}    — upload HTTP JPEG legado
  GET  /snapshot/{mac}/latest|list|{seq}|burst/...
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

from db import init_db, close_db
from http_routes import build_app
from models import active_connections
from mqtt_client import run_mqtt
from tcp_image_server import run_tcp_image_server

logging.basicConfig(
    level=logging.INFO,
    format="%(name)-22s | %(levelname)-8s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("fluxcam.server")

HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", 8080))


async def session_watchdog() -> None:
    """Marca devices como offline se não reportarem em 120s."""
    while True:
        await asyncio.sleep(30)
        now = time.time()
        for mac, conn in list(active_connections.items()):
            if now - conn.last_seen > 120 and conn.online:
                conn.online = False
                logger.warning("Device %s marked offline (no activity for >120s)", mac)


async def main() -> None:
    logger.info("=== FluxCam Server starting ===")

    await init_db()
    logger.info("PostgreSQL ready")

    app = build_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    logger.info("HTTP API listening on http://%s:%d", HOST, PORT)

    watchdog_task   = asyncio.create_task(session_watchdog())
    mqtt_task       = asyncio.create_task(run_mqtt())
    tcp_image_task  = asyncio.create_task(run_tcp_image_server())

    logger.info("=== FluxCam Server ready — Ctrl+C to stop ===")
    try:
        await asyncio.Future()
    finally:
        for task in (watchdog_task, mqtt_task, tcp_image_task):
            task.cancel()
        await asyncio.gather(watchdog_task, mqtt_task, tcp_image_task, return_exceptions=True)
        await runner.cleanup()
        await close_db()
        logger.info("FluxCam Server stopped.")


if __name__ == "__main__":
    # paho-mqtt (usado pelo aiomqtt) requer SelectorEventLoop no Windows.
    # ProactorEventLoop (default no Win32) nao suporta add_reader/add_writer.
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
