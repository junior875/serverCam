from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any
from urllib.parse import urlencode

import aiohttp


def _http_base(base_url: str) -> str:
    return base_url.rstrip("/")


def _ws_base(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[len("https://"):]
    if base.startswith("http://"):
        return "ws://" + base[len("http://"):]
    return base


async def _recv_json(ws: aiohttp.ClientWebSocketResponse, expected_type: str | None = None) -> dict[str, Any]:
    msg = await ws.receive(timeout=10)
    if msg.type != aiohttp.WSMsgType.TEXT:
        raise RuntimeError(f"unexpected WS message type: {msg.type}")
    data = json.loads(msg.data)
    if expected_type and data.get("type") != expected_type:
        raise RuntimeError(f"expected type={expected_type}, got={data}")
    return data


async def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke test do protocolo HiveGrid-CameraOS")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--mac", default="AA:BB:CC:DD:EE:FF")
    parser.add_argument("--firmware", default="2.1.0")
    parser.add_argument("--device-id", default="ccddeeff")
    parser.add_argument("--seq", type=int, default=101)
    args = parser.parse_args()

    ws_url = _ws_base(args.base_url) + "/"
    upload_url = _http_base(args.base_url) + "/upload?" + urlencode(
        {"device_id": args.device_id, "filename": str(args.seq)}
    )

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(ws_url, protocols=("arduino",), heartbeat=15) as ws:
            await ws.send_json(
                {
                    "type": "register",
                    "mac": args.mac,
                    "firmware": args.firmware,
                    "caps": ["flash_main", "flash_monitor", "snapshot", "burst", "ota"],
                }
            )

            registered = await _recv_json(ws, "registered")
            session_key = registered["session_key"]
            print("registered:", registered)

            init_msg = await _recv_json(ws, "init")
            print("init:", init_msg)

            await ws.send_json({"type": "ping", "seq": 1})
            pong = await _recv_json(ws, "pong")
            print("pong:", pong)

            await ws.send_json(
                {
                    "type": "status",
                    "seq": 2,
                    "uptime": 3600,
                    "rssi": -65,
                    "heap": 180000,
                    "link_score": 85,
                    "pkt_loss": 2.3,
                    "avg_rtt": 120,
                }
            )
            status_ack = await _recv_json(ws, "ack")
            print("status ack:", status_ack)

            jpeg_bytes = b"\xff\xd8\xff\xdbSMOKEJPEG\xff\xd9"
            async with session.post(
                upload_url,
                data=jpeg_bytes,
                headers={
                    "Content-Type": "application/octet-stream",
                    "X-Device-Mac": args.mac,
                    "X-Session-Key": session_key,
                    "X-Seq": str(args.seq),
                    "X-Width": "640",
                    "X-Height": "480",
                    "X-Quality": "9",
                },
            ) as resp:
                payload = await resp.json()
                if resp.status != 200:
                    raise RuntimeError(f"upload failed: status={resp.status} body={payload}")
                print("upload:", payload)

            await ws.send_json(
                {
                    "type": "snapshot_done",
                    "seq": args.seq,
                    "size": len(jpeg_bytes),
                    "width": 640,
                    "height": 480,
                }
            )
            snapshot_ack = await _recv_json(ws, "ack")
            print("snapshot ack:", snapshot_ack)


if __name__ == "__main__":
    asyncio.run(main())
