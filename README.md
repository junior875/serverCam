# FluxCam Server

Servidor WebSocket + API HTTP para gerenciar câmeras ESP32.
Feito com Python 3.11, aiohttp e PostgreSQL.

---

## Estrutura

```
server_cam_v4/
├── server.py           → ponto de entrada
├── ws_handler.py       → protocolo WebSocket com as câmeras
├── http_routes.py      → endpoints HTTP + autenticação
├── db.py               → PostgreSQL (asyncpg)
├── models.py           → estado em memória
├── requirements.txt
├── Procfile            → deploy no Railway
├── .env.example        → modelo de variáveis de ambiente
└── thinker/
    └── thinker.py      → CLI para enviar comandos às câmeras
```

---

## Rodar localmente

### 1. Pré-requisitos

- Python 3.11+
- PostgreSQL rodando (pode usar Docker: `docker run -e POSTGRES_PASSWORD=senha -p 5432:5432 postgres`)

### 2. Configurar o ambiente

```bash
cp .env.example .env
```

Edite o `.env`:

```
DATABASE_URL=postgresql://postgres:senha@localhost:5432/fluxcam
PORT=8080
API_KEY=minhaChaveSuperSecreta
```

### 3. Instalar dependências e rodar

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python server.py
```

O servidor sobe em `ws://localhost:8080/` (WebSocket) e `http://localhost:8080/` (HTTP API).

---

## Deploy no Railway

1. Crie um projeto no [Railway](https://railway.app)
2. Conecte o repositório Git
3. Adicione o plugin **PostgreSQL** — Railway preenche `DATABASE_URL` automaticamente
4. Em **Variables**, adicione:
   - `API_KEY` → uma string longa e aleatória (ex: gere com `python -c "import secrets; print(secrets.token_hex(24))"`)
5. Deploy. Railway usa o `Procfile` (`web: python server.py`) e expõe a porta via `PORT`.

---

## Como a câmera ESP32 conecta

A câmera usa WebSocket. O endereço de conexão é a raiz do servidor:

- **Local:** `ws://IP_DO_SERVIDOR:8080/`
- **Railway:** `wss://SEU-APP.up.railway.app/`

> Atenção: o path é `/` (raiz). Não existe `/ws` nem nenhuma sub-rota.

### Fluxo de conexão

```
1. Câmera abre WebSocket em ws://HOST/
2. Câmera envia "register" com MAC, firmware e caps
3. Servidor responde "registered" com session_key e config
4. Servidor envia "init" com a config salva no banco
5. Câmera começa a mandar "ping" periodicamente
6. Câmera manda "status" a cada 60s com a config atual
```

### Mensagem de registro (câmera → servidor)

```json
{
  "type": "register",
  "mac": "C0:4E:30:0A:C4:BC",
  "firmware": "1.0.0",
  "caps": ["flash_main", "flash_monitor"]
}
```

> `flash_monitor` só entra em `caps` se o hardware tiver esse recurso.

### Resposta do servidor

```json
{
  "type": "registered",
  "session_key": "abc123...",
  "config": {
    "servo_h": 90,
    "servo_v": 90,
    "flash_main": 0,
    "flash_monitor": 0,
    "ping_interval": 30
  }
}
```

Logo depois o servidor envia um `"type": "init"` com o mesmo conteúdo.

### Escala do flash

`flash_main` e `flash_monitor` são inteiros de **0 a 10**:
- `0` = desligado
- `10` = brilho máximo

---

## API HTTP

Todos os endpoints HTTP exigem autenticação.

**Header:** `X-API-Key: SUA_CHAVE`
**OU query string:** `?api_key=SUA_CHAVE`

### `GET /devices`

Lista todos os dispositivos registrados.

```bash
curl http://localhost:8080/devices -H "X-API-Key: minhaChave"
```

Resposta:
```json
[
  {
    "mac": "C0:4E:30:0A:C4:BC",
    "firmware": "1.0.0",
    "status": "online",
    "last_seen": 1713220800.0,
    "config": {
      "servo_h": 90, "servo_v": 90,
      "flash_main": 0, "flash_monitor": 0,
      "ping_interval": 30
    }
  }
]
```

### `POST /command/{mac}`

Envia um comando tipado para a câmera.

**Tipos aceitos:** `init`, `start_stream`, `update_config`

Exemplo — atualizar config:
```bash
curl -X POST http://localhost:8080/command/C0:4E:30:0A:C4:BC \
  -H "X-API-Key: minhaChave" \
  -H "Content-Type: application/json" \
  -d '{"type": "update_config", "config": {"servo_h": 120, "flash_main": 7}}'
```

### `POST /simulate/{mac}`

Envia qualquer JSON livre como comando `cmd` para a câmera.

```bash
curl -X POST http://localhost:8080/simulate/C0:4E:30:0A:C4:BC \
  -H "X-API-Key: minhaChave" \
  -H "Content-Type: application/json" \
  -d '{"servo_h": 45, "servo_v": 30}'
```

---

## CLI Thinker (atalho interativo)

Para não precisar digitar curl, use o CLI:

```powershell
python thinker\thinker.py
```

Se o servidor estiver em outro host:

```powershell
python thinker\thinker.py --host SEU-APP.up.railway.app --port 443
```

> O thinker não precisa de dependências extras — usa só stdlib Python.
