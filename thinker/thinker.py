"""
FluxCam Thinker — CLI interativo para o FluxCam Server
Usa apenas a API HTTP (porta 8080). Não conecta no WebSocket.

Uso:
    python thinker.py
    python thinker.py --host 192.168.1.100 --port 8080
    python thinker.py --url https://xxx.trycloudflare.com
    (ou defina THINKER_BASE_URL no .env da raiz do projeto)
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# ─── Config ───────────────────────────────────────────────────────────────────

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080

_api_key: str = ""  # preenchido em main()


def _load_dotenv() -> dict:
    """Lê o .env do diretório pai do thinker (raiz do projeto) sem dependências externas."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    values = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        values[key.strip()] = val.strip()
    return values


def _normalize_base_url(url: str) -> str:
    """Garante URL sem barra final. Aceita https://host ou http://host:port."""
    u = url.strip().rstrip("/")
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


def _resolve_base_url(dotenv: dict, args) -> str:
    """Prioridade: --url > env THINKER_BASE_URL > .env THINKER_BASE_URL > --host/--port."""
    from_env = os.environ.get("THINKER_BASE_URL", "").strip()
    from_file = (dotenv.get("THINKER_BASE_URL") or "").strip()
    if getattr(args, "url", None) and args.url.strip():
        return _normalize_base_url(args.url)
    if from_env:
        return _normalize_base_url(from_env)
    if from_file:
        return _normalize_base_url(from_file)
    return f"http://{args.host}:{args.port}"


# ─── HTTP helpers ─────────────────────────────────────────────────────────────

def _headers(extra: dict | None = None) -> dict:
    h = {"Content-Type": "application/json"}
    if _api_key:
        h["X-API-Key"] = _api_key
    if extra:
        h.update(extra)
    return h


def _request(method: str, url: str, body: dict | None = None) -> tuple[int, Any]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, headers=_headers(), method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {"error": str(e)}
    except Exception as e:
        return 0, {"error": str(e)}


def _get_binary(url: str) -> tuple[int, bytes, dict]:
    """GET que retorna bytes brutos (para download de imagem)."""
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, b"", {}
    except Exception as e:
        return 0, b"", {}


def get(base: str, path: str):
    return _request("GET", f"{base}{path}")


def post(base: str, path: str, body: dict):
    return _request("POST", f"{base}{path}", body)


# ─── Exibição ─────────────────────────────────────────────────────────────────

def pretty(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


def ok(label: str, data: Any = None):
    print(f"\n  \033[92m✔ {label}\033[0m")
    if data is not None:
        print(pretty(data))


def err(code: int, data: Any):
    print(f"\n  \033[91m✘ HTTP {code}\033[0m")
    print(pretty(data))


def result(code: int, data: Any):
    if 200 <= code < 300:
        ok(f"HTTP {code}", data)
    else:
        err(code, data)


# ─── Ações — config ───────────────────────────────────────────────────────────

def cmd_devices(base: str, _args):
    code, data = get(base, "/devices")
    if not (200 <= code < 300):
        err(code, data); return

    if not data:
        print("\n  (nenhum dispositivo registrado)")
        return

    print()
    for d in data:
        dot = "\033[92m●\033[0m" if d["status"] == "online" else "\033[90m○\033[0m"
        snap = d.get("snapshots", {})
        snap_info = f"  snapshots={snap.get('count', 0)}" if snap else ""
        print(f"  {dot}  {d['mac']}  [{d['status']}]  fw={d['firmware']}{snap_info}")
        cfg = d.get("config", {})
        if cfg:
            print(f"      servo h={cfg.get('servo_h')}  v={cfg.get('servo_v')}  "
                  f"flash_main={cfg.get('flash_main')}/10  "
                  f"flash_monitor={cfg.get('flash_monitor')}/10  "
                  f"ping={cfg.get('ping_interval')}s")


def cmd_update_config(base: str, _args):
    mac = _pick_mac(base)
    if not mac:
        return

    print("\n  Informe os campos que deseja alterar (Enter = manter atual):")
    fields = {
        "servo_h":       ("Servo H (0-180)", int),
        "servo_v":       ("Servo V (0-180)", int),
        "flash_main":    ("Flash main (0-10)", int),
        "flash_monitor": ("Flash monitor (0-10)", int),
        "ping_interval": ("Ping interval em segundos", int),
    }
    config = {}
    for key, (label, cast) in fields.items():
        raw = input(f"    {label}: ").strip()
        if not raw:
            continue
        try:
            config[key] = cast(raw)
        except ValueError:
            print(f"    ✘ valor inválido para {key}, ignorado")

    if not config:
        print("  Nenhum campo alterado.")
        return

    code, data = post(base, f"/command/{mac}", {"type": "update_config", "config": config})
    result(code, data)


def cmd_init(base: str, _args):
    mac = _pick_mac(base)
    if not mac:
        return
    code, data = post(base, f"/command/{mac}", {"type": "init"})
    result(code, data)


def cmd_simulate(base: str, _args):
    mac = _pick_mac(base)
    if not mac:
        return
    raw = input("\n  body JSON livre (ex: {\"servo_h\": 120}): ").strip()
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  ✘ JSON inválido: {e}")
        return
    code, data = post(base, f"/simulate/{mac}", body)
    result(code, data)


# ─── Ações — snapshot ─────────────────────────────────────────────────────────

def cmd_take_snapshot(base: str, _args):
    """Pede uma foto avulsa à câmera."""
    mac = _pick_mac(base)
    if not mac:
        return

    quality = _ask_int("  Qualidade JPEG (1-100, Enter=70): ", 70, 1, 100)
    width   = _ask_int("  Largura px   (Enter=320): ",          320, 1, 4096)
    height  = _ask_int("  Altura px    (Enter=240): ",          240, 1, 4096)
    seq     = int(time.time()) % 100000  # seq baseado em timestamp

    code, data = post(base, f"/command/{mac}", {
        "type":    "take_snapshot",
        "seq":     seq,
        "quality": quality,
        "width":   width,
        "height":  height,
    })
    result(code, data)

    if 200 <= code < 300:
        print(f"\n  Aguardando câmera fazer upload (seq={seq})...")
        _wait_for_snapshot(base, mac, seq)


def cmd_start_burst(base: str, _args):
    """Inicia captura em rajada (quasi-vídeo)."""
    mac = _pick_mac(base)
    if not mac:
        return

    fps     = _ask_int("  FPS (1-5, Enter=1): ",          1, 1, 5)
    count   = _ask_int("  Qtd frames (0=infinito, Enter=10): ", 10, 0, 9999)
    quality = _ask_int("  Qualidade JPEG (1-100, Enter=50): ",  50, 1, 100)
    width   = _ask_int("  Largura px (Enter=320): ",            320, 1, 4096)
    height  = _ask_int("  Altura px  (Enter=240): ",            240, 1, 4096)

    code, data = post(base, f"/command/{mac}", {
        "type":    "start_burst",
        "fps":     fps,
        "count":   count,
        "quality": quality,
        "width":   width,
        "height":  height,
    })
    result(code, data)

    if 200 <= code < 300:
        burst_id = data.get("burst_id", "?")
        print(f"\n  Burst iniciado!  burst_id={burst_id}")
        print(f"  Use [7] Parar burst quando quiser parar.")
        print(f"  Use [9] Ver frames do burst para baixar as fotos.")


def cmd_stop_burst(base: str, _args):
    mac = _pick_mac(base)
    if not mac:
        return
    code, data = post(base, f"/command/{mac}", {"type": "stop_burst"})
    result(code, data)


def cmd_latest_snapshot(base: str, _args):
    """Baixa o último snapshot e abre no visualizador do sistema."""
    mac = _pick_mac(base)
    if not mac:
        return

    print(f"\n  Baixando último snapshot de {mac}...")
    code, raw, headers = _get_binary(f"{base}/snapshot/{mac}/latest")

    if code == 404:
        print("  ✘ Nenhum snapshot disponível ainda.")
        return
    if code != 200 or not raw:
        print(f"  ✘ Erro HTTP {code}")
        return

    seq  = headers.get("X-Seq", "?")
    size = headers.get("X-Size", len(raw))
    ok(f"Snapshot seq={seq}  {size} bytes")

    _open_image(raw, f"fluxcam_{mac.replace(':', '')}_{seq}.jpg")


def cmd_list_snapshots(base: str, _args):
    """Lista todos os frames em memória para um device."""
    mac = _pick_mac(base)
    if not mac:
        return

    code, data = get(base, f"/snapshot/{mac}/list")
    if not (200 <= code < 300):
        err(code, data); return

    stats  = data.get("stats", {})
    frames = data.get("frames", [])

    print(f"\n  {mac} — {stats.get('count', 0)} frames em memória")
    if stats.get("total_bytes"):
        print(f"  total: {stats['total_bytes'] // 1024} KB  "
              f"seq {stats.get('oldest_seq')}→{stats.get('latest_seq')}")

    if not frames:
        print("  (nenhum frame)")
        return

    print()
    for f in frames[-20:]:  # mostra últimos 20
        burst = f"  burst={f['burst_id']}" if f.get("burst_id") else ""
        ts    = time.strftime("%H:%M:%S", time.localtime(f["timestamp"]))
        print(f"    seq={f['seq']:5d}  {f['size']:7d}B  {ts}{burst}")

    # Pergunta se quer baixar algum
    raw = input("\n  Baixar seq (Enter = pular): ").strip()
    if not raw:
        return
    try:
        seq = int(raw)
    except ValueError:
        return

    code, data, headers = _get_binary(f"{base}/snapshot/{mac}/{seq}")
    if code != 200 or not data:
        print(f"  ✘ Erro HTTP {code}")
        return

    ok(f"seq={seq}  {len(data)} bytes")
    _open_image(data, f"fluxcam_{mac.replace(':', '')}_{seq}.jpg")


def cmd_burst_frames(base: str, _args):
    """Baixa e exibe lista de frames de um burst específico."""
    mac = _pick_mac(base)
    if not mac:
        return

    # Mostra lista para o usuário escolher o burst_id
    code, list_data = get(base, f"/snapshot/{mac}/list")
    if not (200 <= code < 300):
        err(code, list_data); return

    burst_ids = sorted({
        f["burst_id"] for f in list_data.get("frames", []) if f.get("burst_id")
    })

    if not burst_ids:
        print("  ✘ Nenhum burst encontrado.")
        return

    print("\n  Bursts disponíveis:")
    for i, bid in enumerate(burst_ids):
        print(f"    [{i}] {bid}")

    raw = input("  Escolha: ").strip()
    try:
        burst_id = burst_ids[int(raw)]
    except (ValueError, IndexError):
        print("  ✘ Escolha inválida."); return

    code, data = get(base, f"/snapshot/{mac}/burst/{burst_id}/list")
    result(code, data)


# ─── Utilitários internos ─────────────────────────────────────────────────────

def _ask_int(prompt: str, default: int, lo: int, hi: int) -> int:
    raw = input(prompt).strip()
    if not raw:
        return default
    try:
        v = int(raw)
        return max(lo, min(hi, v))
    except ValueError:
        return default


def _pick_mac(base: str) -> str | None:
    code, devices = get(base, "/devices")
    if not (200 <= code < 300) or not devices:
        print("  ✘ Não foi possível listar dispositivos.")
        return None

    online = [d for d in devices if d["status"] == "online"]
    if not online:
        print("  ✘ Nenhuma câmera online no momento.")
        return None

    if len(online) == 1:
        mac = online[0]["mac"]
        print(f"  → {mac}")
        return mac

    print("\n  Câmeras online:")
    for i, d in enumerate(online):
        print(f"    [{i}] {d['mac']}")
    raw = input("  Escolha: ").strip()
    try:
        return online[int(raw)]["mac"]
    except (ValueError, IndexError):
        print("  ✘ Escolha inválida.")
        return None


def _wait_for_snapshot(base: str, mac: str, seq: int, timeout: int = 15):
    """Polling simples: verifica se o seq apareceu no store."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        code, data, headers = _get_binary(f"{base}/snapshot/{mac}/{seq}")
        if code == 200 and data:
            size = len(data)
            ok(f"Upload recebido!  seq={seq}  {size} bytes")
            abrir = input("  Abrir imagem? (Enter=sim / n): ").strip().lower()
            if abrir != "n":
                _open_image(data, f"fluxcam_{mac.replace(':', '')}_{seq}.jpg")
            return
    print(f"  ⚠ Timeout — snapshot seq={seq} não chegou em {timeout}s.")
    print(f"  Tente baixar depois com [8] Ver snapshots.")


def _open_image(data: bytes, filename: str):
    """Salva em temp e abre com o visualizador padrão do OS."""
    path = os.path.join(tempfile.gettempdir(), filename)
    with open(path, "wb") as f:
        f.write(data)
    print(f"  Salvo em: {path}")
    try:
        if sys.platform == "win32":
            os.startfile(path)
        elif sys.platform == "darwin":
            subprocess.run(["open", path])
        else:
            subprocess.run(["xdg-open", path])
    except Exception as e:
        print(f"  (não foi possível abrir automaticamente: {e})")


# ─── Menu ─────────────────────────────────────────────────────────────────────

MENU = [
    # label                              função
    ("Listar dispositivos",              cmd_devices),
    ("Atualizar config",                 cmd_update_config),
    ("Reenviar init",                    cmd_init),
    ("Simular comando livre",            cmd_simulate),
    ("─── Snapshot ───────────────",    None),
    ("Tirar foto (take_snapshot)",       cmd_take_snapshot),
    ("Iniciar burst (quasi-vídeo)",      cmd_start_burst),
    ("Parar burst",                      cmd_stop_burst),
    ("Ver / baixar último snapshot",     cmd_latest_snapshot),
    ("Listar todos os snapshots",        cmd_list_snapshots),
    ("Frames de um burst",               cmd_burst_frames),
]


def run(base: str):
    print(f"\n\033[1mFluxCam Thinker\033[0m  →  {base}")

    while True:
        print("\n" + "─" * 46)
        for i, (label, fn) in enumerate(MENU):
            if fn is None:
                print(f"  {label}")
            else:
                print(f"  [{i}] {label}")
        print("  [q] Sair")
        print("─" * 46)

        choice = input("  Opção: ").strip().lower()
        if choice == "q":
            print("  Saindo.")
            break

        try:
            idx = int(choice)
            label, fn = MENU[idx]
        except (ValueError, IndexError):
            print("  ✘ Opção inválida.")
            continue

        if fn is None:
            print("  ✘ Opção inválida.")
            continue

        fn(base, {})


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    global _api_key

    # Carrega .env do projeto antes de parsear args
    dotenv = _load_dotenv()

    parser = argparse.ArgumentParser(description="FluxCam Thinker CLI")
    parser.add_argument(
        "--url",
        default="",
        help="URL base HTTPS (Cloudflare/Railway), ex: https://xxx.trycloudflare.com",
    )
    parser.add_argument("--host",    default=dotenv.get("HOST", DEFAULT_HOST))
    parser.add_argument("--port",    type=int, default=int(dotenv.get("PORT", DEFAULT_PORT)))
    parser.add_argument("--api-key", default=os.environ.get("API_KEY") or dotenv.get("API_KEY", ""))
    args = parser.parse_args()

    _api_key = args.api_key
    base = _resolve_base_url(dotenv, args)

    try:
        run(base)
    except KeyboardInterrupt:
        print("\n  Interrompido.")
        sys.exit(0)


if __name__ == "__main__":
    main()
