/**
 * FluxCam — Firmware Simulado ESP32
 * Versão 1.1.0 — Snapshot Mode
 *
 * ── Mensagens que a câmera ENVIA ──────────────────────────────────────────
 *  register        → primeira mensagem, sem session_key
 *  ping            → keepalive periódico
 *  status          → estado atual + config atual (servidor persiste)
 *  photo_done      → captura avulsa concluída (legado)
 *  snapshot_ready  → snapshot uploadado com sucesso
 *  snapshot_error  → falha ao capturar ou fazer upload
 *  burst_done      → burst concluído
 *  error           → erro genérico
 *
 * ── Mensagens que a câmera RECEBE ────────────────────────────────────────
 *  registered      → session_key + config inicial
 *  init            → (re)aplica config completa
 *  pong            → resposta ao ping
 *  cmd             → comando livre vindo de /simulate
 *  start_stream    → inicia stream (placeholder)
 *  update_config   → atualiza campos de config
 *  take_snapshot   → tira uma foto e faz upload
 *  start_burst     → inicia captura em rafaga
 *  stop_burst      → para captura em rafaga
 *
 * ── Snapshot upload ────────────────────────────────────────────────────────
 *  Camera faz HTTP POST para SERVER_HTTP_URL/snapshot/{mac}
 *  Headers enviados:
 *    X-Session-Key  → autenticação
 *    X-Seq          → número de sequência do frame
 *    X-Burst-Id     → ID do burst (vazio se snapshot avulso)
 *    X-Width        → largura em pixels
 *    X-Height       → altura em pixels
 *    X-Quality      → qualidade JPEG (0-100)
 *    Content-Type   → image/jpeg
 *  Após upload bem-sucedido, envia snapshot_ready via WS.
 *
 * ── WebSocket ─────────────────────────────────────────────────────────────
 *  Path:  /          (raiz — servidor não tem sub-rota)
 *  Local: ws://HOST:8080/
 *  Cloud: wss://XXX.up.railway.app/
 *
 * ── Dependências (Library Manager) ────────────────────────────────────────
 *   - ArduinoJson  >= 7.x  (Benoit Blanchon)
 *   - WebSockets   >= 2.4  (Markus Sattler / Links2004)
 */

#include <WiFi.h>
#include <HTTPClient.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>

// ─── Configuração de rede ──────────────────────────────────────────────────

const char* WIFI_SSID = "SUA_REDE";
const char* WIFI_PASS = "SUA_SENHA";

// ── URL base do servidor (sem barra final) ──
// Local:
// const char* SERVER_HTTP_URL = "http://192.168.1.100:8080";
// const char* WS_HOST         = "192.168.1.100";
// const uint16_t WS_PORT      = 8080;
// const bool     WS_SSL       = false;

// Cloudflare / Railway:
const char* SERVER_HTTP_URL = "https://buried-recovered-specialty-laundry.trycloudflare.com";
const char*    WS_HOST      = "buried-recovered-specialty-laundry.trycloudflare.com";
const uint16_t WS_PORT      = 443;
const bool     WS_SSL       = true;

const char* WS_PATH      = "/";
const char* FIRMWARE_VER = "1.1.0-sim";

// ─── Capacidades ──────────────────────────────────────────────────────────

const char* DEVICE_CAPS[] = { "flash_main", "flash_monitor", "snapshot" };
const int   DEVICE_CAPS_N = 3;

// ─── Configuração da câmera ────────────────────────────────────────────────

// Tamanho de frame padrão (usado na simulação e para informar o servidor)
// Hardware real: FRAMESIZE_QVGA=320x240, FRAMESIZE_VGA=640x480
const int DEFAULT_WIDTH   = 320;
const int DEFAULT_HEIGHT  = 240;
const int DEFAULT_QUALITY = 60;   // 0-100 (JPEG quality)

// Upload
const int UPLOAD_TIMEOUT_MS  = 8000;   // timeout por tentativa
const int UPLOAD_MAX_RETRIES = 3;      // tentativas antes de desistir
const int UPLOAD_RETRY_MS    = 1500;   // espera entre tentativas

// Burst
const int BURST_MAX_FRAMES   = 120;    // segurança: máximo absoluto de frames por burst
const int BURST_MIN_INTERVAL = 200;    // intervalo mínimo entre capturas (ms)

// ─── Estado do dispositivo ────────────────────────────────────────────────

struct Config {
  int servo_h        = 90;
  int servo_v        = 90;
  int flash_main     = 0;   // 0 = desligado, 10 = máximo brilho
  int flash_monitor  = 0;
  int ping_interval  = 30;  // segundos
};

Config cfg;
char   session_key[64] = "";
bool   ws_connected    = false;
bool   registered      = false;

// Snapshot avulso
int    snap_pending_seq = -1;   // -1 = nenhum pendente
int    snap_quality     = DEFAULT_QUALITY;
int    snap_width       = DEFAULT_WIDTH;
int    snap_height      = DEFAULT_HEIGHT;

// Burst
bool   burst_active     = false;
char   burst_id[32]     = "";
int    burst_fps        = 1;
int    burst_count      = 0;    // 0 = infinito até stop_burst
int    burst_done_count = 0;
int    burst_seq        = 0;
int    burst_quality    = DEFAULT_QUALITY;
int    burst_width      = DEFAULT_WIDTH;
int    burst_height     = DEFAULT_HEIGHT;
unsigned long burst_interval_ms  = 1000;
unsigned long burst_last_ms      = 0;

// Keepalive
unsigned long last_ping_ms   = 0;
unsigned long last_status_ms = 0;

WebSocketsClient ws;

// ─── Helpers ───────────────────────────────────────────────────────────────

String getMac() {
  uint8_t raw[6];
  WiFi.macAddress(raw);
  char buf[18];
  snprintf(buf, sizeof(buf), "%02X:%02X:%02X:%02X:%02X:%02X",
           raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]);
  return String(buf);
}

void sendJson(JsonDocument& doc) {
  String out;
  serializeJson(doc, out);
  ws.sendTXT(out);
  Serial.println("[TX] " + out);
}

void sendError(const char* msg) {
  JsonDocument doc;
  doc["type"]    = "error";
  doc["message"] = msg;
  sendJson(doc);
}

// ─── Câmera simulada ──────────────────────────────────────────────────────
//
// Em hardware real, substitua captureFakeJpeg() por:
//   camera_fb_t* fb = esp_camera_fb_get();
//   // use fb->buf, fb->len, fb->width, fb->height
//   esp_camera_fb_return(fb);
//
// O JPEG mínimo válido abaixo é 1x1 pixel cinza, apenas para o
// código de upload funcionar sem hardware real.

static const uint8_t FAKE_JPEG[] = {
  0xFF,0xD8,0xFF,0xE0,0x00,0x10,0x4A,0x46,0x49,0x46,0x00,0x01,0x01,0x00,0x00,0x01,
  0x00,0x01,0x00,0x00,0xFF,0xDB,0x00,0x43,0x00,0x08,0x06,0x06,0x07,0x06,0x05,0x08,
  0x07,0x07,0x07,0x09,0x09,0x08,0x0A,0x0C,0x14,0x0D,0x0C,0x0B,0x0B,0x0C,0x19,0x12,
  0x13,0x0F,0x14,0x1D,0x1A,0x1F,0x1E,0x1D,0x1A,0x1C,0x1C,0x20,0x24,0x2E,0x27,0x20,
  0x22,0x2C,0x23,0x1C,0x1C,0x28,0x37,0x29,0x2C,0x30,0x31,0x34,0x34,0x34,0x1F,0x27,
  0x39,0x3D,0x38,0x32,0x3C,0x2E,0x33,0x34,0x32,0xFF,0xC0,0x00,0x0B,0x08,0x00,0x01,
  0x00,0x01,0x01,0x01,0x11,0x00,0xFF,0xC4,0x00,0x1F,0x00,0x00,0x01,0x05,0x01,0x01,
  0x01,0x01,0x01,0x01,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x01,0x02,0x03,0x04,
  0x05,0x06,0x07,0x08,0x09,0x0A,0x0B,0xFF,0xC4,0x00,0xB5,0x10,0x00,0x02,0x01,0x03,
  0x03,0x02,0x04,0x03,0x05,0x05,0x04,0x04,0x00,0x00,0x01,0x7D,0x01,0x02,0x03,0x00,
  0x04,0x11,0x05,0x12,0x21,0x31,0x41,0x06,0x13,0x51,0x61,0x07,0x22,0x71,0x14,0x32,
  0x81,0x91,0xA1,0x08,0x23,0x42,0xB1,0xC1,0x15,0x52,0xD1,0xF0,0x24,0x33,0x62,0x72,
  0x82,0x09,0x0A,0x16,0x17,0x18,0x19,0x1A,0x25,0x26,0x27,0x28,0x29,0x2A,0x34,0x35,
  0x36,0x37,0x38,0x39,0x3A,0x43,0x44,0x45,0x46,0x47,0x48,0x49,0x4A,0x53,0x54,0x55,
  0x56,0x57,0x58,0x59,0x5A,0x63,0x64,0x65,0x66,0x67,0x68,0x69,0x6A,0x73,0x74,0x75,
  0x76,0x77,0x78,0x79,0x7A,0x83,0x84,0x85,0x86,0x87,0x88,0x89,0x8A,0x92,0x93,0x94,
  0x95,0x96,0x97,0x98,0x99,0x9A,0xA2,0xA3,0xA4,0xA5,0xA6,0xA7,0xA8,0xA9,0xAA,0xB2,
  0xB3,0xB4,0xB5,0xB6,0xB7,0xB8,0xB9,0xBA,0xC2,0xC3,0xC4,0xC5,0xC6,0xC7,0xC8,0xC9,
  0xCA,0xD2,0xD3,0xD4,0xD5,0xD6,0xD7,0xD8,0xD9,0xDA,0xE1,0xE2,0xE3,0xE4,0xE5,0xE6,
  0xE7,0xE8,0xE9,0xEA,0xF1,0xF2,0xF3,0xF4,0xF5,0xF6,0xF7,0xF8,0xF9,0xFA,0xFF,0xDA,
  0x00,0x08,0x01,0x01,0x00,0x00,0x3F,0x00,0xFB,0xD4,0xFF,0xD9
};
static const size_t FAKE_JPEG_LEN = sizeof(FAKE_JPEG);

// ─── Upload de snapshot ───────────────────────────────────────────────────
//
// Robusto para 4G: timeout configurável, retry com backoff, sem bloquear WS.
//
// Para hardware real, substitua buf/len pelo frame da câmera.

bool uploadSnapshot(int seq, const char* burstId,
                    const uint8_t* buf, size_t len,
                    int w, int h, int quality) {

  char url[256];
  snprintf(url, sizeof(url), "%s/snapshot/%s",
           SERVER_HTTP_URL, getMac().c_str());

  char seqStr[12], wStr[8], hStr[8], qStr[8];
  snprintf(seqStr, sizeof(seqStr), "%d", seq);
  snprintf(wStr,   sizeof(wStr),   "%d", w);
  snprintf(hStr,   sizeof(hStr),   "%d", h);
  snprintf(qStr,   sizeof(qStr),   "%d", quality);

  for (int attempt = 1; attempt <= UPLOAD_MAX_RETRIES; attempt++) {
    HTTPClient http;
    http.begin(url);
    http.setTimeout(UPLOAD_TIMEOUT_MS);
    http.addHeader("Content-Type",   "image/jpeg");
    http.addHeader("X-Session-Key",  session_key);
    http.addHeader("X-Seq",          seqStr);
    http.addHeader("X-Burst-Id",     burstId ? burstId : "");
    http.addHeader("X-Width",        wStr);
    http.addHeader("X-Height",       hStr);
    http.addHeader("X-Quality",      qStr);

    int code = http.POST((uint8_t*)buf, len);
    http.end();

    if (code == 200) {
      Serial.printf("[UP] seq=%d size=%u OK (tentativa %d)\n", seq, len, attempt);
      return true;
    }

    Serial.printf("[UP] seq=%d tentativa %d falhou HTTP %d — aguardando %dms\n",
                  seq, attempt, code, UPLOAD_RETRY_MS);

    if (attempt < UPLOAD_MAX_RETRIES) {
      delay(UPLOAD_RETRY_MS);
    }
  }

  Serial.printf("[UP] seq=%d UPLOAD FALHOU após %d tentativas\n",
                seq, UPLOAD_MAX_RETRIES);
  return false;
}

// ─── Captura + upload (bloqueia o loop por ~upload_time) ──────────────────
//
// Para 4G sem travar: mover uploadSnapshot() para uma FreeRTOS task separada
// usando xTaskCreatePinnedToCore() e uma fila (xQueueSend) para passar o frame.
// Isso permite continuar recebendo WS enquanto o upload acontece em paralelo.
//
// Exemplo de uso com tarefa (para implementar futuramente):
//   xQueueSend(uploadQueue, &frameDesc, 0);   // não bloqueia

void captureAndUpload(int seq, const char* burstId, int w, int h, int quality) {
  Serial.printf("[CAM] capturando frame seq=%d %dx%d q=%d burst=%s\n",
                seq, w, h, quality, burstId ? burstId : "avulso");

  // ── Hardware real: ─────────────────────────────────────────────
  // sensor_t* s = esp_camera_sensor_get();
  // s->set_quality(s, quality);
  // s->set_framesize(s, FRAMESIZE_QVGA);   // ajustar por w/h
  // camera_fb_t* fb = esp_camera_fb_get();
  // if (!fb) { sendSnapshotError(seq, "camera_fb_get failed"); return; }
  // bool ok = uploadSnapshot(seq, burstId, fb->buf, fb->len, fb->width, fb->height, quality);
  // esp_camera_fb_return(fb);
  // ────────────────────────────────────────────────────────────────

  // Simulação:
  bool ok = uploadSnapshot(seq, burstId, FAKE_JPEG, FAKE_JPEG_LEN, w, h, quality);

  if (ok) {
    // Notifica servidor via WS
    JsonDocument doc;
    doc["type"]     = "snapshot_ready";
    doc["seq"]      = seq;
    doc["size"]     = (int)FAKE_JPEG_LEN;
    doc["burst_id"] = burstId ? burstId : "";
    sendJson(doc);
  } else {
    JsonDocument doc;
    doc["type"]    = "snapshot_error";
    doc["seq"]     = seq;
    doc["message"] = "upload failed after retries";
    sendJson(doc);
  }
}

// ─── Aplicar configuração ─────────────────────────────────────────────────

void applyConfig(JsonObjectConst obj) {
  if (obj["servo_h"].is<int>()) {
    cfg.servo_h = obj["servo_h"].as<int>();
    Serial.printf("[CFG] servo_h → %d\n", cfg.servo_h);
  }
  if (obj["servo_v"].is<int>()) {
    cfg.servo_v = obj["servo_v"].as<int>();
    Serial.printf("[CFG] servo_v → %d\n", cfg.servo_v);
  }
  if (obj["flash_main"].is<int>()) {
    cfg.flash_main = constrain(obj["flash_main"].as<int>(), 0, 10);
    Serial.printf("[CFG] flash_main → %d/10\n", cfg.flash_main);
    // hardware: ledcWrite(FLASH_MAIN_CH, map(cfg.flash_main, 0, 10, 0, 255));
  }
  if (obj["flash_monitor"].is<int>()) {
    cfg.flash_monitor = constrain(obj["flash_monitor"].as<int>(), 0, 10);
    Serial.printf("[CFG] flash_monitor → %d/10\n", cfg.flash_monitor);
  }
  if (obj["ping_interval"].is<int>()) {
    cfg.ping_interval = obj["ping_interval"].as<int>();
    Serial.printf("[CFG] ping_interval → %ds\n", cfg.ping_interval);
  }
}

// ─── Handlers de comandos do servidor ────────────────────────────────────

void handleRegistered(JsonObjectConst msg) {
  const char* key = msg["session_key"];
  if (key) {
    strncpy(session_key, key, sizeof(session_key) - 1);
    session_key[sizeof(session_key) - 1] = '\0';
  }
  registered = true;
  Serial.printf("[AUTH] session_key=%s\n", session_key);
  if (msg["config"].is<JsonObjectConst>()) applyConfig(msg["config"].as<JsonObjectConst>());
}

void handleInit(JsonObjectConst msg) {
  Serial.println("[CMD] init — aplicando config");
  if (msg["config"].is<JsonObjectConst>()) applyConfig(msg["config"].as<JsonObjectConst>());
}

void handleCmd(JsonObjectConst msg) {
  Serial.println("[CMD] cmd — aplicando campos");
  applyConfig(msg);
}

void handleUpdateConfig(JsonObjectConst msg) {
  Serial.println("[CMD] update_config");
  if (msg["config"].is<JsonObjectConst>()) applyConfig(msg["config"].as<JsonObjectConst>());
}

void handleStartStream(JsonObjectConst msg) {
  Serial.println("[CMD] start_stream (placeholder)");
}

// ── Snapshot avulso ────────────────────────────────────────────────────────
// Recebe: { "type": "take_snapshot", "seq": 1, "quality": 80,
//           "width": 320, "height": 240 }

void handleTakeSnapshot(JsonObjectConst msg) {
  if (burst_active) {
    Serial.println("[SNAP] ignorado — burst ativo");
    return;
  }
  snap_pending_seq = msg["seq"].is<int>() ? msg["seq"].as<int>() : (snap_pending_seq + 1);
  snap_quality     = msg["quality"].is<int>() ? constrain(msg["quality"].as<int>(), 1, 100) : DEFAULT_QUALITY;
  snap_width       = msg["width"].is<int>()   ? msg["width"].as<int>()   : DEFAULT_WIDTH;
  snap_height      = msg["height"].is<int>()  ? msg["height"].as<int>()  : DEFAULT_HEIGHT;

  Serial.printf("[SNAP] take_snapshot seq=%d q=%d %dx%d\n",
                snap_pending_seq, snap_quality, snap_width, snap_height);

  // Captura imediata (loop() processa no próximo ciclo via flag)
  captureAndUpload(snap_pending_seq, nullptr, snap_width, snap_height, snap_quality);
  snap_pending_seq = -1;
}

// ── Burst mode ─────────────────────────────────────────────────────────────
// Recebe: { "type": "start_burst", "burst_id": "abc123", "fps": 2,
//           "count": 30, "quality": 50, "width": 320, "height": 240 }
//
// fps  → frames por segundo (1–5 recomendado para 4G)
// count → 0 = infinito (até stop_burst)

void handleStartBurst(JsonObjectConst msg) {
  if (burst_active) {
    Serial.println("[BURST] já ativo — ignorado");
    return;
  }

  const char* bid = msg["burst_id"];
  if (bid) strncpy(burst_id, bid, sizeof(burst_id) - 1);
  else     snprintf(burst_id, sizeof(burst_id), "auto_%lu", millis());

  int fps     = msg["fps"].is<int>() ? constrain(msg["fps"].as<int>(), 1, 10) : 1;
  burst_count   = msg["count"].is<int>() ? msg["count"].as<int>() : 0;
  burst_quality = msg["quality"].is<int>() ? constrain(msg["quality"].as<int>(), 1, 100) : DEFAULT_QUALITY;
  burst_width   = msg["width"].is<int>()   ? msg["width"].as<int>()   : DEFAULT_WIDTH;
  burst_height  = msg["height"].is<int>()  ? msg["height"].as<int>()  : DEFAULT_HEIGHT;

  burst_interval_ms = max((unsigned long)(1000UL / fps), (unsigned long)BURST_MIN_INTERVAL);
  burst_active      = true;
  burst_done_count  = 0;
  burst_seq         = 0;
  burst_last_ms     = 0;  // dispara imediatamente no próximo loop

  Serial.printf("[BURST] start burst_id=%s fps=%d interval=%lums count=%d q=%d %dx%d\n",
                burst_id, fps, burst_interval_ms, burst_count,
                burst_quality, burst_width, burst_height);
}

void handleStopBurst(JsonObjectConst msg) {
  if (!burst_active) return;
  burst_active = false;

  Serial.printf("[BURST] stop — %d frames capturados (burst_id=%s)\n",
                burst_done_count, burst_id);

  JsonDocument doc;
  doc["type"]       = "burst_done";
  doc["burst_id"]   = burst_id;
  doc["count"]      = burst_done_count;
  sendJson(doc);
}

// ─── Envio periódico ──────────────────────────────────────────────────────

void sendPing() {
  JsonDocument doc;
  doc["type"] = "ping";
  sendJson(doc);
}

void sendStatus() {
  JsonDocument doc;
  doc["type"]      = "status";
  doc["uptime"]    = millis() / 1000;
  doc["rssi"]      = WiFi.RSSI();
  doc["streaming"] = false;
  doc["burst"]     = burst_active;

  JsonObject c    = doc["config"].to<JsonObject>();
  c["servo_h"]    = cfg.servo_h;
  c["servo_v"]    = cfg.servo_v;
  c["flash_main"] = cfg.flash_main;

  bool hasFM = false;
  for (int i = 0; i < DEVICE_CAPS_N; i++) {
    if (strcmp(DEVICE_CAPS[i], "flash_monitor") == 0) { hasFM = true; break; }
  }
  if (hasFM) c["flash_monitor"] = cfg.flash_monitor;
  c["ping_interval"] = cfg.ping_interval;

  sendJson(doc);
}

// ─── WebSocket event handler ──────────────────────────────────────────────

void onWsEvent(WStype_t evType, uint8_t* payload, size_t len) {
  switch (evType) {

    case WStype_CONNECTED:
      ws_connected = true;
      Serial.println("[WS] Conectado — enviando register");
      {
        JsonDocument reg;
        reg["type"]     = "register";
        reg["mac"]      = getMac();
        reg["firmware"] = FIRMWARE_VER;
        JsonArray caps  = reg["caps"].to<JsonArray>();
        for (int i = 0; i < DEVICE_CAPS_N; i++) caps.add(DEVICE_CAPS[i]);
        sendJson(reg);
      }
      break;

    case WStype_DISCONNECTED:
      ws_connected = false;
      registered   = false;
      memset(session_key, 0, sizeof(session_key));
      if (burst_active) {
        burst_active = false;
        Serial.println("[BURST] cancelado por desconexão");
      }
      Serial.println("[WS] Desconectado — aguardando reconexão");
      break;

    case WStype_TEXT: {
      Serial.println("[RX] " + String((char*)payload));

      JsonDocument doc;
      if (deserializeJson(doc, payload, len) != DeserializationError::Ok) {
        Serial.println("[ERR] JSON inválido");
        break;
      }

      const char* t = doc["type"] | "";

      if      (strcmp(t, "registered")    == 0) handleRegistered(doc.as<JsonObjectConst>());
      else if (strcmp(t, "init")          == 0) handleInit(doc.as<JsonObjectConst>());
      else if (strcmp(t, "pong")          == 0) { /* silencioso */ }
      else if (strcmp(t, "cmd")           == 0) handleCmd(doc.as<JsonObjectConst>());
      else if (strcmp(t, "update_config") == 0) handleUpdateConfig(doc.as<JsonObjectConst>());
      else if (strcmp(t, "start_stream")  == 0) handleStartStream(doc.as<JsonObjectConst>());
      else if (strcmp(t, "take_snapshot") == 0) handleTakeSnapshot(doc.as<JsonObjectConst>());
      else if (strcmp(t, "start_burst")   == 0) handleStartBurst(doc.as<JsonObjectConst>());
      else if (strcmp(t, "stop_burst")    == 0) handleStopBurst(doc.as<JsonObjectConst>());
      else Serial.printf("[WS] tipo ignorado: %s\n", t);

      break;
    }

    case WStype_ERROR:
      Serial.println("[WS] Erro de socket");
      break;

    default:
      break;
  }
}

// ─── Setup ────────────────────────────────────────────────────────────────

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.printf("\n=== FluxCam Firmware %s ===\n", FIRMWARE_VER);
  Serial.println("MAC: " + getMac());

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);
  Serial.print("[WiFi] Conectando");
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
  }
  Serial.println("\n[WiFi] IP: " + WiFi.localIP().toString());

  if (WS_SSL) ws.beginSSL(WS_HOST, WS_PORT, WS_PATH);
  else        ws.begin(WS_HOST, WS_PORT, WS_PATH);

  ws.onEvent(onWsEvent);
  ws.setReconnectInterval(3000);
}

// ─── Loop ─────────────────────────────────────────────────────────────────

void loop() {
  ws.loop();

  if (!ws_connected || !registered) return;

  unsigned long now = millis();

  // ── Burst mode ──
  if (burst_active) {
    bool countOk = (burst_count == 0) || (burst_done_count < burst_count);
    bool timeOk  = (now - burst_last_ms >= burst_interval_ms);

    if (countOk && timeOk) {
      burst_last_ms = now;
      burst_seq++;
      burst_done_count++;

      captureAndUpload(burst_seq, burst_id, burst_width, burst_height, burst_quality);

      // Limite de segurança absoluto
      if (burst_done_count >= BURST_MAX_FRAMES) {
        Serial.println("[BURST] limite máximo atingido — parando");
        handleStopBurst(JsonObjectConst());
      } else if (burst_count > 0 && burst_done_count >= burst_count) {
        // count definido e atingido
        handleStopBurst(JsonObjectConst());
      }
    }
    // Não envia ping/status durante burst para não poluir a fila de rede
    return;
  }

  // ── Ping periódico ──
  if (now - last_ping_ms >= (unsigned long)cfg.ping_interval * 1000UL) {
    last_ping_ms = now;
    sendPing();
  }

  // ── Status periódico ──
  if (now - last_status_ms >= 60000UL) {
    last_status_ms = now;
    sendStatus();
  }
}
