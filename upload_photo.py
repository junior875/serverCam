"""
Envia um JPEG local para o servidor como se fosse a câmera.
Uso:
    python upload_photo.py <caminho_foto> [--base-url URL] [--mac MAC]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiohttp


def _ws_url(base: str) -> str:
    base = base.rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[len("https://"):]
    if base.startswith("http://"):
        return "ws://" + base[len("http://"):]
    return base


async def main() -> None:
    parser = argparse.ArgumentParser(description="Upload de foto para o servidor")
    parser.add_argument("photo", help="Caminho para o arquivo JPEG")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--mac", default="AA:BB:CC:DD:EE:FF")
    parser.add_argument("--seq", type=int, default=1)
    args = parser.parse_args()

    photo_path = Path(args.photo)
    if not photo_path.exists():
        print(f"Arquivo nao encontrado: {photo_path}")
        sys.exit(1)

    jpeg_bytes = photo_path.read_bytes()
    chip_id = "".join(c for c in args.mac if c.isalnum()).lower()[-8:]

    print(f"Foto:     {photo_path.name}  ({len(jpeg_bytes)} bytes)")
    print(f"Servidor: {args.base_url}")
    print(f"MAC:      {args.mac}  chip_id={chip_id}")
    print()

    async with aiohttp.ClientSession() as session:
        # 1. Conecta WS e registra para obter session_key
        ws_url = _ws_url(args.base_url) + "/"
        async with session.ws_connect(ws_url, protocols=("arduino",), heartbeat=15) as ws:
            await ws.send_json({
                "type": "register",
                "mac": args.mac,
                "firmware": "2.1.0",
                "caps": ["flash_main", "flash_monitor", "snapshot", "burst", "ota"],
            })

            # Aguarda registered
            msg = await ws.receive(timeout=10)
            registered = json.loads(msg.data)
            assert registered["type"] == "registered", f"Esperado registered, got {registered}"
            session_key = registered["session_key"]
            print(f"[WS] registered  session_key={session_key}")

            # Aguarda init
            msg = await ws.receive(timeout=5)
            init = json.loads(msg.data)
            print(f"[WS] init        config={init.get('config')}")

            # 2. Faz o upload HTTP da foto
            upload_url = (
                f"{args.base_url.rstrip('/')}/upload"
                f"?device_id={chip_id}&filename={args.seq}"
            )
            async with session.post(
                upload_url,
                data=jpeg_bytes,
                headers={
                    "Content-Type": "image/jpeg",
                    "X-Device-Mac": args.mac,
                    "X-Session-Key": session_key,
                    "X-Seq": str(args.seq),
                },
            ) as resp:
                body = await resp.json()
                if resp.status != 200:
                    print(f"[HTTP] ERRO {resp.status}: {body}")
                    sys.exit(1)
                print(f"[HTTP] upload OK  seq={body['seq']}  size={body['size']} bytes")

            # 3. Notifica snapshot_done via WS
            await ws.send_json({
                "type": "snapshot_done",
                "seq": args.seq,
                "size": len(jpeg_bytes),
            })
            msg = await ws.receive(timeout=5)
            ack = json.loads(msg.data)
            print(f"[WS] ack         {ack}")

    print()
    print("Foto salva em: fotos/<mac>/<single ou burst_id>/<seq>.jpg")


if __name__ == "__main__":
    asyncio.run(main())
