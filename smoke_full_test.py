"""
smoke_full_test.py — Teste completo do protocolo MQTT + TCP binário

Simula uma câmera ESP32 completa:
  1. MQTT register  →  recebe registered + init
  2. MQTT ping      →  recebe pong
  3. MQTT status    →  servidor ACK
  4. HTTP POST /command/{mac} para take_snapshot  (via API)
  5. TCP binário HG\x01\x00 com JPEG real (18-04-2026_08-36-15.jpg)
  6. MQTT snapshot_done
  7. HTTP GET /snapshot/{mac}/latest  — verifica se chegou

Uso:
  python smoke_full_test.py [--image caminho.jpg] [--http http://localhost:8080]
                            [--mqtt-host localhost] [--mqtt-port 1883]
                            [--tcp-host localhost] [--tcp-port 9000]
                            [--api-key SUA_CHAVE]
"""
from __future__ import annotations

import argparse
import asyncio
import io
import json
import struct
import sys
import time
from pathlib import Path

# Força UTF-8 no stdout do Windows para evitar cp1252 UnicodeEncodeError
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import aiomqtt
import aiohttp

# ─── Configuração padrão ──────────────────────────────────────────────────────

DEFAULT_IMAGE  = str(Path(__file__).parent / "18-04-2026_08-36-15.jpg")
DEFAULT_HTTP   = "http://localhost:8080"
DEFAULT_MQTT   = "localhost"
DEFAULT_MQTT_P = 1883
DEFAULT_TCP    = "localhost"
DEFAULT_TCP_P  = 9000

# MAC fictício da câmera de teste
CAM_MAC        = "1C:DB:D4:47:F9:94"
CAM_DEVICE_ID  = "d447f994"           # últimos 8 hex do MAC
CAM_FIRMWARE   = "2.1.0"
CAM_CAPS       = ["flash_main", "flash_monitor", "snapshot", "burst"]

MAGIC = b"HG\x01\x00"

# ─── Helpers de output ────────────────────────────────────────────────────────

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg: str)   -> None: print(f"  {GREEN}OK{RESET}  {msg}")
def fail(msg: str) -> None: print(f"  {RED}FAIL{RESET} {msg}")
def info(msg: str) -> None: print(f"  {CYAN}...{RESET} {msg}")
def step(msg: str) -> None: print(f"\n{BOLD}{YELLOW}[STEP]{RESET} {msg}")


# ─── 1. MQTT: register → registered + init ───────────────────────────────────

async def test_mqtt_register(args) -> tuple[str | None, dict | None]:
    """Publica register e coleta registered + init do tópico down."""
    step("MQTT - register -> registered + init")
    down_topic = f"hivegrid/{CAM_MAC}/down"
    up_topic   = f"hivegrid/{CAM_MAC}/up"

    session_key = None
    init_msg    = None

    try:
        async with aiomqtt.Client(
            hostname   = args.mqtt_host,
            port       = args.mqtt_port,
            identifier = "smoke-cam-test",
            keepalive  = 30,
        ) as client:
            await client.subscribe(down_topic)
            info(f"subscribed to {down_topic}")

            # Publica register
            payload = json.dumps({
                "type":     "register",
                "mac":      CAM_MAC,
                "firmware": CAM_FIRMWARE,
                "caps":     CAM_CAPS,
            })
            await client.publish(up_topic, payload.encode())
            info(f"published register — mac={CAM_MAC}")

            received = {}
            deadline = asyncio.get_event_loop().time() + 5.0
            async with asyncio.timeout(5.0):
                async for msg in client.messages:
                    data = json.loads(msg.payload)
                    received[data.get("type")] = data
                    if "registered" in received and "init" in received:
                        break
                    if asyncio.get_event_loop().time() > deadline:
                        break

            if "registered" in received:
                session_key = received["registered"].get("session_key")
                ok(f"registered — session_key={session_key}")
            else:
                fail("registered NOT received within 5s")

            if "init" in received:
                ok(f"init — {received['init']}")
                init_msg = received["init"]
            else:
                fail("init NOT received within 5s")

    except Exception as exc:
        fail(f"MQTT register error: {exc}")

    return session_key, init_msg


# ─── 2. MQTT: ping → pong ────────────────────────────────────────────────────

async def test_mqtt_ping(args) -> bool:
    step("MQTT — ping → pong")
    down_topic = f"hivegrid/{CAM_MAC}/down"
    up_topic   = f"hivegrid/{CAM_MAC}/up"
    ping_seq   = 42

    try:
        async with aiomqtt.Client(
            hostname   = args.mqtt_host,
            port       = args.mqtt_port,
            identifier = "smoke-cam-ping",
            keepalive  = 30,
        ) as client:
            await client.subscribe(down_topic)

            t0 = time.monotonic()
            await client.publish(up_topic, json.dumps({"type": "ping", "seq": ping_seq}).encode())
            info(f"published ping seq={ping_seq}")

            async with asyncio.timeout(5.0):
                async for msg in client.messages:
                    data = json.loads(msg.payload)
                    if data.get("type") == "pong":
                        rtt_ms = (time.monotonic() - t0) * 1000
                        if data.get("seq") == ping_seq:
                            ok(f"pong seq={data['seq']} lq_score={data.get('lq_score')} RTT={rtt_ms:.1f}ms")
                            return True
                        else:
                            fail(f"pong seq mismatch: expected {ping_seq} got {data.get('seq')}")
                            return False
    except asyncio.TimeoutError:
        fail("pong NOT received within 5s")
    except Exception as exc:
        fail(f"MQTT ping error: {exc}")
    return False


# ─── 3. MQTT: status ─────────────────────────────────────────────────────────

async def test_mqtt_status(args) -> bool:
    step("MQTT — status")
    up_topic = f"hivegrid/{CAM_MAC}/up"

    try:
        async with aiomqtt.Client(
            hostname   = args.mqtt_host,
            port       = args.mqtt_port,
            identifier = "smoke-cam-status",
            keepalive  = 30,
        ) as client:
            payload = json.dumps({
                "type":       "status",
                "uptime":     120,
                "rssi":       -42,
                "heap":       207068,
                "link_score": 97,
                "pkt_loss":   0.0,
                "avg_rtt":    38,
            })
            await client.publish(up_topic, payload.encode())
            ok("status published (server processes silently)")
            return True
    except Exception as exc:
        fail(f"MQTT status error: {exc}")
    return False


# ─── 4. HTTP: dispara take_snapshot via API ───────────────────────────────────

async def test_http_take_snapshot(args) -> int | None:
    step("HTTP — POST /command/{mac} take_snapshot")
    url = f"{args.http_base}/command/{CAM_MAC.replace(':', '%3A')}"
    headers = {"X-API-Key": args.api_key, "Content-Type": "application/json"}
    body    = {"type": "take_snapshot", "width": 640, "height": 480, "quality": 9}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=headers) as resp:
                data = await resp.json()
                if resp.status == 200 and data.get("status") == "sent":
                    # O servidor retorna {"status":"sent","mac":...,"type":...}
                    # O seq do comando foi injetado internamente; usamos seq=1 como default
                    ok(f"take_snapshot command sent — resp={data}")
                    return data.get("seq", 1)
                else:
                    fail(f"HTTP {resp.status}: {data}")
    except Exception as exc:
        fail(f"HTTP take_snapshot error: {exc}")
    return None


# ─── 5. TCP binário: upload do JPEG real ─────────────────────────────────────

async def test_tcp_image_upload(args, jpeg_path: str, seq: int = 1) -> bool:
    step(f"TCP binary — upload JPEG: {jpeg_path}")

    img = Path(jpeg_path)
    if not img.exists():
        fail(f"image file not found: {jpeg_path}")
        return False

    jpeg_data = img.read_bytes()
    info(f"image size: {len(jpeg_data):,} bytes")

    if jpeg_data[:2] != b"\xff\xd8":
        fail(f"not a valid JPEG (SOI={jpeg_data[:2]!r})")
        return False

    # Monta header de 76 bytes
    header = bytearray(76)
    header[0:4]  = MAGIC
    mac_bytes    = CAM_MAC.encode("ascii")
    header[4:4+len(mac_bytes)]   = mac_bytes           # MAC (18 bytes)
    dev_bytes    = CAM_DEVICE_ID.encode("ascii")
    header[22:22+len(dev_bytes)] = dev_bytes            # device_id (8 bytes)
    filename     = f"{seq:06d}".encode("ascii")
    header[30:30+len(filename)]  = filename             # filename (32 bytes)
    struct.pack_into(">I",  header, 62, seq)            # SEQ uint32
    struct.pack_into(">H",  header, 66, 640)            # WIDTH uint16
    struct.pack_into(">H",  header, 68, 480)            # HEIGHT uint16
    header[70]   = 9                                    # QUALITY
    header[71]   = 0x00                                 # TYPE=snapshot
    struct.pack_into(">I",  header, 72, len(jpeg_data)) # JPEG_LEN uint32

    info(f"connecting to {args.tcp_host}:{args.tcp_port} ...")
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(args.tcp_host, args.tcp_port),
            timeout=10.0,
        )

        t0 = time.monotonic()
        writer.write(bytes(header))
        writer.write(jpeg_data)
        await asyncio.wait_for(writer.drain(), timeout=15.0)
        info(f"sent {76 + len(jpeg_data):,} bytes (header + JPEG)")

        response = await asyncio.wait_for(reader.readexactly(2), timeout=10.0)
        elapsed  = (time.monotonic() - t0) * 1000

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        if response == b"OK":
            ok(f"server responded OK in {elapsed:.0f}ms")
            return True
        else:
            fail(f"server responded: {response!r}")
            return False

    except asyncio.TimeoutError:
        fail("TCP timeout")
    except ConnectionRefusedError:
        fail(f"TCP connection refused — is server running on port {args.tcp_port}?")
    except Exception as exc:
        fail(f"TCP error: {exc}")
    return False


# ─── 6. MQTT: snapshot_done ───────────────────────────────────────────────────

async def test_mqtt_snapshot_done(args, seq: int = 1, size: int = 0) -> bool:
    step("MQTT — snapshot_done")
    up_topic = f"hivegrid/{CAM_MAC}/up"

    try:
        async with aiomqtt.Client(
            hostname   = args.mqtt_host,
            port       = args.mqtt_port,
            identifier = "smoke-cam-done",
            keepalive  = 30,
        ) as client:
            payload = json.dumps({
                "type":   "snapshot_done",
                "seq":    seq,
                "size":   size,
                "width":  640,
                "height": 480,
            })
            await client.publish(up_topic, payload.encode())
            ok(f"snapshot_done published seq={seq} size={size}")
            return True
    except Exception as exc:
        fail(f"MQTT snapshot_done error: {exc}")
    return False


# ─── 7. HTTP: verifica imagem no servidor ────────────────────────────────────

async def test_http_latest_snapshot(args) -> bool:
    step("HTTP — GET /snapshot/{mac}/latest")
    url     = f"{args.http_base}/snapshot/{CAM_MAC.replace(':', '%3A')}/latest"
    headers = {"X-API-Key": args.api_key}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    body = await resp.read()
                    seq  = resp.headers.get("X-Seq", "?")
                    size = resp.headers.get("X-Size", "?")
                    ok(f"latest snapshot seq={seq} size={size} ({len(body):,} bytes JPEG)")
                    return True
                else:
                    txt = await resp.text()
                    fail(f"HTTP {resp.status}: {txt[:100]}")
    except Exception as exc:
        fail(f"HTTP latest error: {exc}")
    return False


# ─── HTTP: lista devices ──────────────────────────────────────────────────────

async def test_http_devices(args) -> bool:
    step("HTTP — GET /devices")
    url     = f"{args.http_base}/devices"
    headers = {"X-API-Key": args.api_key}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                devices = await resp.json()
                if resp.status == 200:
                    cam = next((d for d in devices if d["mac"] == CAM_MAC), None)
                    if cam:
                        ok(f"device found: status={cam['status']} transport={cam.get('transport')} "
                           f"last_image_seq={cam.get('last_image_seq')}")
                    else:
                        info(f"cam {CAM_MAC} not listed yet (may need broker to be live)")
                    return True
                else:
                    fail(f"HTTP {resp.status}")
    except Exception as exc:
        fail(f"HTTP devices error: {exc}")
    return False


# ─── Runner principal ─────────────────────────────────────────────────────────

async def run(args) -> None:
    print(f"\n{BOLD}{'-'*60}")
    print(f"  FluxCam Smoke Test - MQTT + TCP binario")
    print(f"  MQTT  : {args.mqtt_host}:{args.mqtt_port}")
    print(f"  TCP   : {args.tcp_host}:{args.tcp_port}")
    print(f"  HTTP  : {args.http_base}")
    print(f"  Image : {args.image}")
    print(f"{'-'*60}{RESET}\n")

    results = {}

    # Fase 1: MQTT handshake
    session_key, _ = await test_mqtt_register(args)
    results["register"] = session_key is not None

    results["ping"]   = await test_mqtt_ping(args)
    results["status"] = await test_mqtt_status(args)

    # Fase 2: dispara take_snapshot via HTTP API
    snap_seq = await test_http_take_snapshot(args)
    results["take_snapshot_cmd"] = snap_seq is not None
    seq = snap_seq or 1

    # Fase 3: upload TCP binário com JPEG real
    jpeg_size = Path(args.image).stat().st_size if Path(args.image).exists() else 0
    results["tcp_upload"] = await test_tcp_image_upload(args, args.image, seq=seq)

    # Fase 4: confirma via MQTT snapshot_done
    results["snapshot_done"] = await test_mqtt_snapshot_done(args, seq=seq, size=jpeg_size)

    # Fase 5: verifica no servidor
    results["latest_http"] = await test_http_latest_snapshot(args)
    results["devices_http"] = await test_http_devices(args)

    # Resumo
    print(f"\n{BOLD}{'-'*60}")
    print("  RESUMO")
    print(f"{'-'*60}{RESET}")
    passed = 0
    for name, result in results.items():
        icon = f"{GREEN}PASS{RESET}" if result else f"{RED}FAIL{RESET}"
        print(f"  {icon}  {name}")
        if result:
            passed += 1
    total = len(results)
    color = GREEN if passed == total else (YELLOW if passed > total // 2 else RED)
    print(f"\n  {color}{BOLD}{passed}/{total} passed{RESET}\n")
    sys.exit(0 if passed == total else 1)


def main() -> None:
    p = argparse.ArgumentParser(description="FluxCam MQTT+TCP smoke test")
    p.add_argument("--image",      default=DEFAULT_IMAGE,   help="Caminho do JPEG real")
    p.add_argument("--http-base",  default=DEFAULT_HTTP,    dest="http_base")
    p.add_argument("--mqtt-host",  default=DEFAULT_MQTT,    dest="mqtt_host")
    p.add_argument("--mqtt-port",  default=DEFAULT_MQTT_P,  dest="mqtt_port", type=int)
    p.add_argument("--tcp-host",   default=DEFAULT_TCP,     dest="tcp_host")
    p.add_argument("--tcp-port",   default=DEFAULT_TCP_P,   dest="tcp_port",  type=int)
    p.add_argument("--api-key",    default="troque_por_chave_forte_aqui", dest="api_key")
    args = p.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
