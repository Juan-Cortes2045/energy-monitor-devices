// =============================================================================
// EnergyMonitor - ESP32 + PZEM-004T V3 metering device
// =============================================================================
// Publishes electrical telemetry to an Eclipse Mosquitto broker following the
// contract defined in README.md.
//
// SRS requirements:
//   FRS5.1  send voltage, current, active power and accumulated energy
//           every 60 seconds over MQTT
//   FRS5.2  identify itself with a unique identifier and access key
//   FRS5.3  online/offline presence derived from the last received reading
//   FRS5.4  automatic reconnection without user intervention
//
// Topology (deployment.md): the device only publishes and never receives
// commands ("the backend never calls the device", scope.md:38). It only opens
// outbound connections, so it works behind any home router without opening
// ports: home Wi-Fi -> internet -> broker (TLS) -> backend.
//
// Linking: the web app does it over Bluetooth LE (setup mode, see config.h
// section 2). The Wi-Fi portal stays as a fallback. config.h only carries
// defaults and hardware settings.
//
// Build with the "Minimal SPIFFS" partition scheme (min_spiffs): with Bluetooth
// the binary does not fit in the 1.2 MB of the default scheme. The NVS lives at
// the same address, so the saved configuration survives the change.
// =============================================================================

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include <WiFiManager.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <PZEM004Tv30.h>
#include <NimBLEDevice.h>
#include <time.h>

#include "config.h"
#include "ca_certs.h"
#if __has_include("dev_ca.h")
#include "dev_ca.h"
#define HAS_DEV_CA 1
#endif

// -----------------------------------------------------------------------------
// Device settings (NVS)
// -----------------------------------------------------------------------------
// Sizes = columns of the `device` table + terminator.
struct DeviceSettings {
  char mqttHost[65];
  char mqttPort[6];
  char deviceId[11];
  char deviceCode[7];
  char apiKey[101];
  // The home network, kept here as well as in the Wi-Fi driver: reconnecting with
  // the driver's own copy (WiFi.begin() without arguments) was refused by some
  // phone hotspots (reason 2) and the copy was lost when the radio restarted.
  char wifiSsid[33];
  char wifiPass[65];
};

DeviceSettings settings;
Preferences prefs;

char topicTelemetry[80];
char topicStatus[80];
// Commands from the backend (retained): {"cmd":"unlink"} when the module is removed
// from its home in the web app.
char topicCommand[80];
volatile bool unlinkRequested = false;

// -----------------------------------------------------------------------------
// Global objects
// -----------------------------------------------------------------------------
PZEM004Tv30 pzem(Serial2, PZEM_RX_PIN, PZEM_TX_PIN, PZEM_ADDRESS);

#if MQTT_USE_TLS
WiFiClientSecure netClient;
// setCACert keeps the pointer: the bundle must live for the whole program.
String caBundle;
#else
WiFiClient netClient;
#endif
PubSubClient mqtt(netClient);

WiFiManager wm;
WiFiManagerParameter paramHost("mqtt_host", "MQTT server (domain or IP)", "", 64);
WiFiManagerParameter paramPort("mqtt_port", "MQTT port (8883 with TLS)", "", 5);
WiFiManagerParameter paramDeviceId("device_id", "Device ID (id_device)", "", 10);
WiFiManagerParameter paramDeviceCode("device_code", "Device code (device_code)", "", 6);
WiFiManagerParameter paramApiKey("api_key", "Device API key", "", 100);

unsigned long lastTelemetryMs = 0;
unsigned long lastStatusMs = 0;
unsigned long lastReconnectAttemptMs = 0;
unsigned long reconnectDelayMs = RECONNECT_MIN_DELAY_MS;
unsigned long wifiLostSinceMs = 0;
unsigned long lastBlinkMs = 0;

// -----------------------------------------------------------------------------
// Setup mode
// -----------------------------------------------------------------------------
// States the web app sees on the STATUS characteristic (short: a BLE
// notification without a negotiated MTU carries 20 bytes).
enum class Join { Idle, Connecting, WaitingBroker };

// Why setup mode started. With NoWifi the module is already linked and only lost
// the network: the saved network keeps being retried and the portal is not
// opened, because WiFiManager shuts the STA connection down while it is active.
// With Rejected the broker refuses the saved credentials (the device was removed
// from its home in the web app, or its key was replaced): the network is fine,
// so it is kept as well and only Bluetooth is offered, to link it again.
// With NoBroker the network works but the server does not answer (the module
// moved to another network and the saved server address is not reachable from
// it): same, Bluetooth is offered to send the new network and server.
enum class SetupReason { Unlinked, Button, NoWifi, Rejected, NoBroker };

// Consecutive "not authorized" answers of the broker before offering Bluetooth.
#define MQTT_REJECTIONS_FOR_SETUP 3
uint8_t mqttRejections = 0;

// NoWifi and Rejected keep the station connection: no portal. A macro, not a
// function: the Arduino builder would put its prototype above the types it needs.
#define KEEPS_STATION(reason) ((reason) == SetupReason::NoWifi || (reason) == SetupReason::Rejected || \
                               (reason) == SetupReason::NoBroker)

// Wi-Fi up but no broker for this long: offer Bluetooth (NoBroker).
#define BROKER_SETUP_AFTER_MS 180000UL
unsigned long brokerLostSinceMs = 0;

// The BOOT button is watched by an interrupt: a connection attempt to an
// unreachable server blocks loop() for seconds, and polling alone missed holds.
volatile unsigned long buttonPressedAtMs = 0;
volatile bool buttonHoldDone = false;


bool setupMode = false;
SetupReason setupReason = SetupReason::Unlinked;
unsigned long setupUntilMs = 0;
unsigned long lastWifiRetryMs = 0;
uint8_t wifiRetries = 0;
unsigned long setupExitAtMs = 0;       // delayed exit once connected
Join joinState = Join::Idle;
unsigned long joinDeadlineMs = 0;
volatile uint8_t lastDisconnectReason = 0;

NimBLEServer* bleServer = nullptr;
NimBLECharacteristic* bleInfo = nullptr;
NimBLECharacteristic* bleNetworks = nullptr;
NimBLECharacteristic* bleStatus = nullptr;
bool bleStarted = false;

// Written from the NimBLE task, consumed in loop().
String bleConfigBuffer;
String blePendingConfig;
volatile bool bleConfigReady = false;
volatile bool bleScanRequested = false;
// Last list served on NETWORKS. A write on it ("scan again") must not leave the
// written bytes as its value: the web app would read them as the list.
String lastNetworksJson = "[]";
bool scanRunning = false;
uint8_t scanAttempts = 0;

// -----------------------------------------------------------------------------
// Electrical sample
// -----------------------------------------------------------------------------
// storedEnergy: the PZEM returns accumulated kWh, the unit compared against
// home_thresholds (BASE-DE-DATOS.md:521).
struct Measurement {
  uint32_t epoch;      // 0 when the clock was not synchronized at measuring time
  uint32_t takenAtMs;  // millis() when measured: lets the sample be dated once the time arrives
  float voltage;
  float current;
  float activePower;
  float storedEnergy;
  uint16_t frequency;
  float powerFactor;
  int rssi;
};

#if ENABLE_OFFLINE_QUEUE
Measurement offlineQueue[OFFLINE_QUEUE_SIZE];
uint16_t queueHead = 0;
uint16_t queueSize = 0;

void enqueueOffline(const Measurement& m) {
  if (queueSize == OFFLINE_QUEUE_SIZE) {
    // Buffer full: the oldest sample is dropped. Recent data wins because the
    // backend derives the online state from the last received reading (FRS5.3).
    offlineQueue[queueHead] = m;
    queueHead = (queueHead + 1) % OFFLINE_QUEUE_SIZE;
  } else {
    offlineQueue[(queueHead + queueSize) % OFFLINE_QUEUE_SIZE] = m;
    queueSize++;
  }
}

bool dequeueOffline(Measurement& m) {
  if (queueSize == 0) {
    return false;
  }
  m = offlineQueue[queueHead];
  queueHead = (queueHead + 1) % OFFLINE_QUEUE_SIZE;
  queueSize--;
  return true;
}
#endif

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------
void logLine(const String& msg) {
  Serial.println(msg);
}

void blink(unsigned int times, unsigned int durationMs) {
  for (unsigned int i = 0; i < times; i++) {
    digitalWrite(LED_PIN, HIGH);
    delay(durationMs);
    digitalWrite(LED_PIN, LOW);
    delay(durationMs);
  }
}

bool clockValid() {
  return time(nullptr) >= MIN_VALID_EPOCH;
}

void formatIso8601(uint32_t epoch, char* out, size_t size) {
  time_t t = (time_t)epoch;
  struct tm tmValue;
  gmtime_r(&t, &tmValue);
  strftime(out, size, "%Y-%m-%dT%H:%M:%SZ", &tmValue);
}

void copyField(char* dst, size_t size, const char* src) {
  strncpy(dst, src ? src : "", size - 1);
  dst[size - 1] = '\0';
  // Values come from a form: trailing whitespace is removed.
  for (int i = (int)strlen(dst) - 1; i >= 0 && isspace((unsigned char)dst[i]); i--) {
    dst[i] = '\0';
  }
}

// -----------------------------------------------------------------------------
// Settings: load, save and apply
// -----------------------------------------------------------------------------
void loadSettings() {
  prefs.begin("energymon", true);
  copyField(settings.mqttHost, sizeof(settings.mqttHost),
            prefs.getString("mqtt_host", DEFAULT_MQTT_HOST).c_str());
  copyField(settings.mqttPort, sizeof(settings.mqttPort),
            prefs.getString("mqtt_port", String(DEFAULT_MQTT_PORT)).c_str());
  copyField(settings.deviceId, sizeof(settings.deviceId),
            prefs.getString("device_id", DEFAULT_DEVICE_ID).c_str());
  copyField(settings.deviceCode, sizeof(settings.deviceCode),
            prefs.getString("device_code", DEFAULT_DEVICE_CODE).c_str());
  copyField(settings.apiKey, sizeof(settings.apiKey),
            prefs.getString("api_key", DEFAULT_DEVICE_API_KEY).c_str());
  copyField(settings.wifiSsid, sizeof(settings.wifiSsid), prefs.getString("wifi_ssid", "").c_str());
  copyField(settings.wifiPass, sizeof(settings.wifiPass), prefs.getString("wifi_pass", "").c_str());
  prefs.end();

  if (strlen(settings.deviceCode) == 0) {
    // A new module needs a code so the web app can identify it. It is derived
    // from the MAC, so it is stable and different on every board.
    uint64_t mac = ESP.getEfuseMac();
    snprintf(settings.deviceCode, sizeof(settings.deviceCode), "EM%02X%02X",
             (uint8_t)(mac >> 32), (uint8_t)(mac >> 40));
  }
}

void saveSettings() {
  prefs.begin("energymon", false);
  prefs.putString("mqtt_host", settings.mqttHost);
  prefs.putString("mqtt_port", settings.mqttPort);
  prefs.putString("device_id", settings.deviceId);
  prefs.putString("device_code", settings.deviceCode);
  prefs.putString("api_key", settings.apiKey);
  prefs.putString("wifi_ssid", settings.wifiSsid);
  prefs.putString("wifi_pass", settings.wifiPass);
  prefs.end();
}

// Joins the saved home network with its name and password, explicitly.
void beginSavedWiFi() {
  if (settings.wifiSsid[0] != '\0') {
    WiFi.begin(settings.wifiSsid, settings.wifiPass);
  } else {
    WiFi.begin();   // configured before 1.3.7: only the driver has the network
  }
}

// Modules configured before 1.3.7 only have the network in the Wi-Fi driver:
// copy it once into the settings.
void adoptDriverNetwork() {
  if (settings.wifiSsid[0] != '\0') {
    return;
  }
  String ssid = WiFi.SSID();
  if (ssid.length() == 0) {
    return;
  }
  copyField(settings.wifiSsid, sizeof(settings.wifiSsid), ssid.c_str());
  copyField(settings.wifiPass, sizeof(settings.wifiPass), WiFi.psk().c_str());
  saveSettings();
  logLine("[CFG] Wi-Fi network '" + ssid + "' kept in the settings");
}

bool settingsComplete() {
  return strlen(settings.mqttHost) > 0 && atoi(settings.mqttPort) > 0 &&
         strlen(settings.deviceId) > 0 && strlen(settings.deviceCode) > 0 &&
         strlen(settings.apiKey) > 0;
}

void refreshBleInfo();

void applySettings() {
  snprintf(topicTelemetry, sizeof(topicTelemetry), "%s%s/telemetry", TOPIC_BASE, settings.deviceId);
  snprintf(topicStatus, sizeof(topicStatus), "%s%s/status", TOPIC_BASE, settings.deviceId);
  snprintf(topicCommand, sizeof(topicCommand), "%s%s/command", TOPIC_BASE, settings.deviceId);
  mqtt.setServer(settings.mqttHost, (uint16_t)atoi(settings.mqttPort));

  // The form shows the current values. The api key is never shown again:
  // leaving the field empty keeps the saved one.
  paramHost.setValue(settings.mqttHost, 64);
  paramPort.setValue(settings.mqttPort, 5);
  paramDeviceId.setValue(settings.deviceId, 10);
  paramDeviceCode.setValue(settings.deviceCode, 6);
  paramApiKey.setValue("", 100);

  logLine("[CFG] deviceId=" + String(settings.deviceId) + " deviceCode=" +
          String(settings.deviceCode) + " broker=" + String(settings.mqttHost) + ":" +
          String(settings.mqttPort) + (MQTT_USE_TLS ? " (TLS)" : " (no TLS)") +
          " apiKey=" + (strlen(settings.apiKey) > 0 ? "set" : "MISSING"));
  refreshBleInfo();
}

// WiFiManager calls this when "Save" is pressed in the portal.
void onPortalSave() {
  copyField(settings.mqttHost, sizeof(settings.mqttHost), paramHost.getValue());
  copyField(settings.mqttPort, sizeof(settings.mqttPort), paramPort.getValue());
  copyField(settings.deviceId, sizeof(settings.deviceId), paramDeviceId.getValue());
  copyField(settings.deviceCode, sizeof(settings.deviceCode), paramDeviceCode.getValue());
  if (strlen(paramApiKey.getValue()) > 0) {
    copyField(settings.apiKey, sizeof(settings.apiKey), paramApiKey.getValue());
  }
  if (wm.getWiFiSSID().length() > 0) {
    copyField(settings.wifiSsid, sizeof(settings.wifiSsid), wm.getWiFiSSID().c_str());
    copyField(settings.wifiPass, sizeof(settings.wifiPass), wm.getWiFiPass().c_str());
  }
  saveSettings();
  logLine("[CFG] settings saved from the portal");
  if (mqtt.connected()) {
    mqtt.disconnect();
  }
  applySettings();
  joinState = Join::WaitingBroker;
  joinDeadlineMs = millis() + WIFI_JOIN_TIMEOUT_MS * 3;
}

// -----------------------------------------------------------------------------
// PZEM sampling
// -----------------------------------------------------------------------------
bool readPzEM(Measurement& out) {
  float voltage = 0.0f;
  float current = 0.0f;
  float power = 0.0f;
  float energy = 0.0f;

  for (int attempt = 1; attempt <= PZEM_READ_RETRIES; attempt++) {
    // PZEM004Tv30 reads the 10 input registers in a single Modbus transaction
    // and caches the result for UPDATE_TIME (200 ms). Later calls to these
    // getters reuse that cache, so 4 getters = 1 UART transaction. Only the
    // first call can return NAN.
    voltage = pzem.voltage();
    current = pzem.current();
    power = pzem.power();
    energy = pzem.energy();

    // The library does not expose error(); it returns NAN when updateValues()
    // fails. The range must be checked too: after a failure the library keeps
    // _lastRead set, so a retry within UPDATE_TIME would return the stale cache
    // (all zeros) instead of asking the sensor again. That is why
    // PZEM_READ_DELAY_MS must exceed UPDATE_TIME.
    bool failed = isnan(voltage) || isnan(current) || isnan(power) || isnan(energy) ||
                  voltage < PZEM_MIN_VALID_VOLTAGE || voltage > PZEM_MAX_VALID_VOLTAGE ||
                  current < 0.0f;

    if (failed) {
      logLine("[PZEM] read failed (attempt " + String(attempt) + "/" +
              String(PZEM_READ_RETRIES) + "), V=" + String(voltage, 1));
      delay(PZEM_READ_DELAY_MS);
      continue;
    }

    out.epoch = clockValid() ? (uint32_t)time(nullptr) : 0;
    out.takenAtMs = millis();
    out.voltage = voltage;
    out.current = current;
    out.activePower = power;
    out.storedEnergy = energy;
    out.frequency = pzem.frequency();
    out.powerFactor = pzem.pf();
    out.rssi = WiFi.RSSI();
    return true;
  }

  // Nothing is published: a sample of zeros would be taken as real consumption
  // and would pollute the home limits.
  logLine("[PZEM] sample discarded: the sensor did not answer");
  return false;
}

// -----------------------------------------------------------------------------
// Payload
// -----------------------------------------------------------------------------
// camelCase to match the backend DTOs.
// Mapping to the `measurement` table:
//   deviceId      -> device_id
//   dateTime      -> date_time      (business timestamp, not created_at)
//   voltage       -> voltage        (V)
//   current       -> current        (A)
//   activePower   -> active_power   (W)
//   storedEnergy  -> stored_energy  (kWh)
// Without a valid time dateTime is omitted and the backend uses the reception
// time, instead of storing the sample in 1970.
// frequency and powerFactor are not in the schema: extra measurements the
// backend ignores. rssi feeds device_status_log.signal_strength.
//
// A sample taken before the clock was set (e.g. right after a power cut) is
// dated when it is sent: current time minus the time elapsed since it was
// measured. Without this every queued sample would arrive with the same time.
uint32_t sampleEpoch(const Measurement& m) {
  if (m.epoch != 0) {
    return m.epoch;
  }
  if (!clockValid()) {
    return 0;
  }
  return (uint32_t)time(nullptr) - (millis() - m.takenAtMs) / 1000;
}

size_t buildTelemetryPayload(uint8_t* buffer, size_t bufferSize, const Measurement& m) {
  JsonDocument doc;
  doc["deviceId"] = settings.deviceId;
  uint32_t epoch = sampleEpoch(m);
  if (epoch != 0) {
    char dateTime[32];
    formatIso8601(epoch, dateTime, sizeof(dateTime));
    doc["dateTime"] = dateTime;
  }
  doc["voltage"] = m.voltage;
  doc["current"] = m.current;
  doc["activePower"] = m.activePower;
  doc["storedEnergy"] = m.storedEnergy;
  doc["frequency"] = m.frequency;
  doc["powerFactor"] = m.powerFactor;
  doc["rssi"] = m.rssi;
  doc["sequence"] = epoch;

  return serializeJson(doc, buffer, bufferSize);
}

size_t buildStatusPayload(uint8_t* buffer, size_t bufferSize, const char* status) {
  JsonDocument doc;
  doc["deviceId"] = settings.deviceId;
  doc["deviceCode"] = settings.deviceCode;
  doc["deviceName"] = DEVICE_NAME;
  doc["location"] = DEVICE_LOCATION;
  doc["status"] = status;
  doc["rssi"] = WiFi.RSSI();
  doc["ip"] = WiFi.localIP().toString();
  doc["firmwareVersion"] = FIRMWARE_VERSION;
  doc["uptimeSeconds"] = millis() / 1000;

  return serializeJson(doc, buffer, bufferSize);
}

// -----------------------------------------------------------------------------
// Bluetooth LE (linking from the web app)
// -----------------------------------------------------------------------------
// Protocol (UUIDs in config.h):
//   INFO      read    {"code","id","fw","cfg"}: module identity
//   NETWORKS  read    [{"s":ssid,"r":rssi,"e":0|1}]: networks the ESP32 sees
//             write   any value: scan again (STATUS -> scan_done)
//   CONFIG    write   JSON {"ssid","pass","host","port","id","key"} in chunks:
//                     '+' + chunk appends, '=' + chunk appends and applies,
//                     '!' clears. It does not depend on the browser supporting
//                     long writes.
//   STATUS    notify  idle | scan_done | wifi_connecting | wifi_ok |
//                     wifi_failed:auth | wifi_failed:notfound | wifi_failed |
//                     mqtt_connecting | mqtt_ok | mqtt_failed:<rc> | bad_config
void bleNotify(const char* status) {
  logLine(String("[BLE] status ") + status);
  if (bleStatus == nullptr) {
    return;
  }
  bleStatus->setValue(status);
  bleStatus->notify();
}

void refreshBleInfo() {
  if (bleInfo == nullptr) {
    return;
  }
  JsonDocument doc;
  doc["code"] = settings.deviceCode;
  doc["id"] = settings.deviceId;
  doc["fw"] = FIRMWARE_VERSION;
  doc["cfg"] = settingsComplete() ? 1 : 0;
  String out;
  serializeJson(doc, out);
  bleInfo->setValue(out);
}

class ConfigCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* characteristic, NimBLEConnInfo& connInfo) override {
    std::string value = characteristic->getValue();
    if (value.empty()) {
      return;
    }
    char op = value[0];
    String chunk = String(value.substr(1).c_str());
    if (op == '!') {
      bleConfigBuffer = "";
    } else if (op == '+') {
      bleConfigBuffer += chunk;
    } else if (op == '=') {
      bleConfigBuffer += chunk;
      blePendingConfig = bleConfigBuffer;
      bleConfigBuffer = "";
      bleConfigReady = true;
    }
    if (bleConfigBuffer.length() > 1024) {
      bleConfigBuffer = "";   // garbage: nobody sends that much
    }
  }
};

class NetworksCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* characteristic, NimBLEConnInfo&) override {
    characteristic->setValue(lastNetworksJson);
    bleScanRequested = true;
  }
};

class ServerCallbacks : public NimBLEServerCallbacks {
  void onDisconnect(NimBLEServer*, NimBLEConnInfo&, int) override {
    if (setupMode) {
      NimBLEDevice::startAdvertising();
    }
  }
};

ConfigCallbacks configCallbacks;
NetworksCallbacks networksCallbacks;
ServerCallbacks serverCallbacks;

void startBle() {
  if (!bleStarted) {
    String name = String(BLE_NAME_PREFIX) + settings.deviceCode;
    NimBLEDevice::init(name.c_str());
    NimBLEDevice::setMTU(247);
    bleServer = NimBLEDevice::createServer();
    bleServer->setCallbacks(&serverCallbacks);
    NimBLEService* service = bleServer->createService(BLE_SERVICE_UUID);
    bleInfo = service->createCharacteristic(BLE_INFO_UUID, NIMBLE_PROPERTY::READ);
    bleNetworks = service->createCharacteristic(BLE_NETWORKS_UUID,
                                                NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::WRITE);
    bleNetworks->setCallbacks(&networksCallbacks);
    NimBLECharacteristic* config = service->createCharacteristic(BLE_CONFIG_UUID, NIMBLE_PROPERTY::WRITE);
    config->setCallbacks(&configCallbacks);
    bleStatus = service->createCharacteristic(BLE_STATUS_UUID, NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::NOTIFY);
    bleStatus->setValue("idle");
    bleNetworks->setValue("[]");
    service->start();

    // A BLE advertisement holds 31 bytes: the 128-bit UUID (18) goes in the main
    // packet, which is what the web app filters on, and the name (2 + 19) in the
    // scan response. enableScanResponse() has to come before setName().
    NimBLEAdvertising* advertising = NimBLEDevice::getAdvertising();
    advertising->enableScanResponse(true);
    advertising->setName(name.c_str());
    if (!advertising->addServiceUUID(BLE_SERVICE_UUID)) {
      logLine("[BLE] the service UUID does not fit in the advertisement");
    }
    bleStarted = true;
    refreshBleInfo();
  }
  bool advertising = NimBLEDevice::startAdvertising();
  logLine(String("[BLE] ") + (advertising ? "advertising '" : "could NOT advertise '") + BLE_NAME_PREFIX +
          settings.deviceCode + "'");
}

void stopBle() {
  if (!bleStarted) {
    return;
  }
  NimBLEDevice::stopAdvertising();
  if (bleServer != nullptr) {
    std::vector<uint16_t> peers = bleServer->getPeerDevices();
    for (uint16_t handle : peers) {
      bleServer->disconnect(handle);
    }
  }
}

void startScan() {
  if (scanRunning) {
    return;
  }
  // The radio cannot scan while the station is joining a network ("sta is
  // connecting, cannot set config") and the scan would report 0 networks: the
  // attempt in progress is dropped first. ensureConnectivity() does not retry
  // while scanRunning and resumes right after the scan.
  if (WiFi.status() != WL_CONNECTED) {
    WiFi.disconnect(false, false);
    delay(100);
  }
  // Asynchronous: the scan takes ~2-4 s and must not hold up measuring.
  if (WiFi.scanNetworks(true, false) == WIFI_SCAN_FAILED) {
    logLine("[WiFi] scan could not start");
  }
  scanRunning = true;
}

void handleScan() {
  if (bleScanRequested) {
    bleScanRequested = false;
    startScan();
  }
  if (!scanRunning) {
    return;
  }
  int16_t found = WiFi.scanComplete();
  if (found == WIFI_SCAN_RUNNING) {
    return;
  }
  scanRunning = false;
  if (found == WIFI_SCAN_FAILED && scanAttempts < 3) {
    scanAttempts++;
    WiFi.scanDelete();
    logLine("[WiFi] scan failed, retrying (" + String(scanAttempts) + "/3)");
    delay(300);
    startScan();
    return;
  }
  scanAttempts = 0;
  lastWifiRetryMs = 0;   // reconnect to the saved network right after the scan
  JsonDocument doc;
  JsonArray list = doc.to<JsonArray>();
  if (found > 0) {
    // Strongest first, without duplicates (a network can have several APs).
    std::vector<int> order;
    for (int i = 0; i < found; i++) {
      order.push_back(i);
    }
    std::sort(order.begin(), order.end(), [](int a, int b) { return WiFi.RSSI(a) > WiFi.RSSI(b); });
    std::vector<String> seen;
    for (int i : order) {
      String ssid = WiFi.SSID(i);
      if (ssid.length() == 0 || std::find(seen.begin(), seen.end(), ssid) != seen.end()) {
        continue;
      }
      seen.push_back(ssid);
      JsonObject net = list.add<JsonObject>();
      net["s"] = ssid;
      net["r"] = WiFi.RSSI(i);
      net["e"] = WiFi.encryptionType(i) == WIFI_AUTH_OPEN ? 0 : 1;
      if ((int)seen.size() >= BLE_MAX_NETWORKS) {
        break;
      }
    }
  }
  WiFi.scanDelete();
  String out;
  serializeJson(doc, out);
  if (bleNetworks != nullptr) {
    bleNetworks->setValue(out);
    lastNetworksJson = out;
  }
  logLine("[WiFi] " + String(list.size()) + " visible network(s)");
  bleNotify("scan_done");
}

// -----------------------------------------------------------------------------
// MQTT
// -----------------------------------------------------------------------------
void publishStatus(const char* status) {
  // uint8_t because the publish() overload with a length requires const uint8_t*.
  uint8_t payload[STATUS_PAYLOAD_SIZE];
  size_t len = buildStatusPayload(payload, sizeof(payload), status);
  if (len == 0) {
    // Publishing an empty payload on a retained topic would clear the state the
    // broker caches. Not publishing is better than publishing nothing.
    logLine("[MQTT] empty status payload, not published");
    return;
  }
  // retain=true: a subscriber that connects later must learn the current state
  // of the device without waiting for the next reading.
  mqtt.publish(topicStatus, payload, len, true);
  lastStatusMs = millis();
}

bool publishTelemetry(const Measurement& m) {
  uint8_t payload[TELEMETRY_PAYLOAD_SIZE];
  size_t len = buildTelemetryPayload(payload, sizeof(payload), m);
  if (len == 0) {
    logLine("[MQTT] empty telemetry payload");
    return false;
  }
  return mqtt.publish(topicTelemetry, payload, len, false);
}

void configureMqtt() {
#if MQTT_USE_TLS
  caBundle = String(LETSENCRYPT_ROOTS);
#ifdef HAS_DEV_CA
  caBundle += DEV_CA_CERT;
  logLine("[TLS] trusting Let's Encrypt and the development CA");
#else
  logLine("[TLS] trusting Let's Encrypt");
#endif
  netClient.setCACert(caBundle.c_str());
  netClient.setHandshakeTimeout(MQTT_SOCKET_TIMEOUT_S);
#endif
  mqtt.setKeepAlive(MQTT_KEEPALIVE_S);
  mqtt.setBufferSize(MQTT_BUFFER_SIZE);
  mqtt.setCallback(onMqttMessage);
  mqtt.setSocketTimeout(MQTT_SOCKET_TIMEOUT_S);
}

const char* mqttStateText(int state) {
  switch (state) {
    case -4: return "timeout";
    case -3: return "connection lost";
    case -2: return "could not open the socket/TLS";
    case -1: return "disconnected";
    case 1:  return "protocol rejected";
    case 2:  return "client id rejected";
    case 3:  return "broker unavailable";
    case 4:  return "wrong username/api key";
    case 5:  return "not authorized";
    default: return "unknown";
  }
}

bool mqttConnect() {
  if (!settingsComplete()) {
    logLine("[MQTT] not linked: configuration missing (setup mode)");
    return false;
  }
#if MQTT_USE_TLS
  if (!clockValid()) {
    // Without the time the validity of the broker certificate cannot be checked.
    logLine("[MQTT] waiting for NTP time to validate the TLS certificate");
    return false;
  }
#endif

  char clientId[32];
  snprintf(clientId, sizeof(clientId), "esp32-%s", settings.deviceId);

  // PubSubClient takes the Last Will as a const char* without length, so it needs
  // a null terminator. serializeJson does not add one, so it is added here.
  uint8_t lwtBytes[STATUS_PAYLOAD_SIZE];
  size_t lwtLen = buildStatusPayload(lwtBytes, sizeof(lwtBytes), "offline");
  char lwt[STATUS_PAYLOAD_SIZE + 1];
  memcpy(lwt, lwtBytes, lwtLen);
  lwt[lwtLen] = '\0';

  // Username = device_code, password = api_key. The broker asks the backend
  // (POST /internal/mqtt/auth/user) and only lets it publish on its own topics.
  bool ok = mqtt.connect(clientId, settings.deviceCode, settings.apiKey, topicStatus, MQTT_QOS, true, lwt);

  if (ok) {
    logLine("[MQTT] connected as " + String(clientId));
    mqtt.subscribe(topicCommand, 1);
    publishStatus("online");
    reconnectDelayMs = RECONNECT_MIN_DELAY_MS;
    mqttRejections = 0;
    if (joinState == Join::WaitingBroker) {
      joinState = Join::Idle;
      bleNotify("mqtt_ok");
      // Give the web app a moment to read the final state.
      setupExitAtMs = millis() + 15000UL;
    } else if (setupMode && KEEPS_STATION(setupReason)) {
      // The saved network (or the credentials) work again: nothing left to configure.
      setupExitAtMs = millis() + 1000UL;
    }
    blink(2, 120);
  } else {
    int state = mqtt.state();
    logLine("[MQTT] connection failed, rc=" + String(state) + " (" + mqttStateText(state) + ")");
#if MQTT_USE_TLS
    char err[96];
    if (netClient.lastError(err, sizeof(err)) != 0) {
      logLine("[TLS] " + String(err));
    }
#endif
    if (joinState == Join::WaitingBroker && (state == 4 || state == 5)) {
      joinState = Join::Idle;
      char status[24];
      snprintf(status, sizeof(status), "mqtt_failed:%d", state);
      bleNotify(status);
    } else if (state == 4 || state == 5) {
      // Wrong credentials never fix themselves: after a few answers in a row the
      // module offers Bluetooth so it can be linked again from the web app.
      if (mqttRejections < 255) {
        mqttRejections++;
      }
      if (mqttRejections >= MQTT_REJECTIONS_FOR_SETUP && !setupMode) {
        enterSetupMode(SetupReason::Rejected, "the server rejects this module (removed from its home?)");
      }
    }
  }
  return ok;
}

#if ENABLE_OFFLINE_QUEUE
void flushOfflineQueue() {
  if (queueSize == 0) {
    return;
  }
  uint16_t flushed = 0;
  Measurement m;
  while (dequeueOffline(m)) {
    if (publishTelemetry(m)) {
      flushed++;
    } else {
      // The sample goes back to the queue and flushing stops until the next
      // stable connection.
      enqueueOffline(m);
      break;
    }
  }
  if (flushed > 0) {
    logLine("[MQTT] offline queue flushed: " + String(flushed) +
            " sample(s), pending=" + String(queueSize));
  }
}
#endif

// -----------------------------------------------------------------------------
// Setup mode: enter, exit and received configuration
// -----------------------------------------------------------------------------
void logWiFi() {
  logLine("[WiFi] connected to '" + WiFi.SSID() + "', IP=" + WiFi.localIP().toString() +
          " RSSI=" + String(WiFi.RSSI()) + "dBm");
}

void startPortal() {
  if (wm.getConfigPortalActive()) {
    return;
  }
  // Non-blocking portal: measuring goes on while it is open.
  wm.setConfigPortalBlocking(false);
  wm.startConfigPortal(PORTAL_SSID, PORTAL_PASSWORD);
}

void enterSetupMode(SetupReason reason, const char* why) {
  setupUntilMs = millis() + SETUP_MODE_TIMEOUT_MS;
  setupExitAtMs = 0;
  // A user gesture (BOOT) or an unlinked module also opens the portal.
  bool withPortal = !KEEPS_STATION(reason);
  if (setupMode) {
    if (withPortal) {
      setupReason = reason;
      startPortal();
    }
    return;
  }
  setupMode = true;
  setupReason = reason;
  startBle();
  // A scan interrupts the connection attempt in progress: with NoWifi it only
  // runs when the web app asks for it (when picking a network), so it never
  // slows down the reconnection to the saved network.
  if (withPortal) {
    startScan();
  }
  if (withPortal) {
    // The portal password is not printed: it is set in secrets.h.
    logLine(String("[SETUP] ") + why + ". Link it from the web app (Bluetooth) or join '" PORTAL_SSID
            "' and open http://192.168.4.1");
    startPortal();
  } else {
    logLine(String("[SETUP] ") + why + ". Still retrying on its own; it can also be "
            "linked from the web app (Bluetooth). Hold BOOT 3 s to open the Wi-Fi portal.");
  }
}

void exitSetupMode(const char* reason) {
  if (!setupMode) {
    return;
  }
  setupMode = false;
  setupExitAtMs = 0;
  if (wm.getConfigPortalActive()) {
    wm.stopConfigPortal();
  }
  WiFi.mode(WIFI_STA);
  stopBle();
  digitalWrite(LED_PIN, LOW);
  logLine(String("[SETUP] done: ") + reason);
  if (WiFi.status() != WL_CONNECTED) {
    // The portal may have left the station off: go back to the saved network.
    beginSavedWiFi();
    lastWifiRetryMs = millis();
  }
}

void applyBleConfig(const String& json) {
  JsonDocument doc;
  if (deserializeJson(doc, json)) {
    bleNotify("bad_config");
    return;
  }
  const char* ssid = doc["ssid"] | "";
  const char* pass = doc["pass"] | "";
  const char* host = doc["host"] | "";
  int port = doc["port"] | 0;
  const char* id = doc["id"] | "";
  const char* key = doc["key"] | "";
  if (strlen(ssid) == 0 || strlen(host) == 0 || port <= 0 || strlen(id) == 0 || strlen(key) == 0) {
    bleNotify("bad_config");
    return;
  }

  copyField(settings.mqttHost, sizeof(settings.mqttHost), host);
  snprintf(settings.mqttPort, sizeof(settings.mqttPort), "%d", port);
  copyField(settings.deviceId, sizeof(settings.deviceId), id);
  copyField(settings.apiKey, sizeof(settings.apiKey), key);
  copyField(settings.wifiSsid, sizeof(settings.wifiSsid), ssid);
  copyField(settings.wifiPass, sizeof(settings.wifiPass), pass);
  saveSettings();
  if (mqtt.connected()) {
    mqtt.disconnect();
  }
  applySettings();
  logLine(String("[SETUP] configuration received over Bluetooth, network '") + ssid + "'");

  // The portal leaves the radio in AP+STA; STA is enough to join the network.
  if (wm.getConfigPortalActive()) {
    wm.stopConfigPortal();
  }
  WiFi.mode(WIFI_STA);
  WiFi.persistent(true);   // the network is saved for the next boots
  // While the station is still trying the old network (or scanning) the radio
  // refuses a new configuration ("sta is connecting, cannot set config") and
  // the new network would never be tried: both are stopped first.
  if (scanRunning) {
    WiFi.scanDelete();
    scanRunning = false;
  }
  WiFi.disconnect(false, false);
  delay(100);
  lastDisconnectReason = 0;
  WiFi.begin(ssid, pass);
  joinState = Join::Connecting;
  joinDeadlineMs = millis() + WIFI_JOIN_TIMEOUT_MS;
  bleNotify("wifi_connecting");
}

void handleJoin() {
  if (joinState == Join::Connecting) {
    if (WiFi.status() == WL_CONNECTED) {
      logWiFi();
      bleNotify("wifi_ok");
      bleNotify("mqtt_connecting");
      joinState = Join::WaitingBroker;
      joinDeadlineMs = millis() + WIFI_JOIN_TIMEOUT_MS * 3;
      lastReconnectAttemptMs = 0;
      reconnectDelayMs = RECONNECT_MIN_DELAY_MS;
      return;
    }
    if ((long)(millis() - joinDeadlineMs) > 0) {
      uint8_t reason = lastDisconnectReason;
      // 15/202/204: handshake or authentication; 201: the network is not found.
      const char* status = (reason == 15 || reason == 202 || reason == 204) ? "wifi_failed:auth"
                           : reason == 201                                  ? "wifi_failed:notfound"
                                                                            : "wifi_failed";
      joinState = Join::Idle;
      logLine("[WiFi] join of '" + WiFi.SSID() + "' failed, status=" + String(WiFi.status()) +
              " reason=" + String(reason));
      WiFi.disconnect(false, false);
      bleNotify(status);
      // The portal is reopened in case the user prefers it.
      if (setupMode && !KEEPS_STATION(setupReason)) {
        startPortal();
      }
    }
  } else if (joinState == Join::WaitingBroker && (long)(millis() - joinDeadlineMs) > 0) {
    joinState = Join::Idle;
    bleNotify("mqtt_failed:-2");
  }
}

void handleSetupMode() {
  if (bleConfigReady) {
    bleConfigReady = false;
    String json = blePendingConfig;
    blePendingConfig = "";
    applyBleConfig(json);
  }
  if (!setupMode) {
    return;
  }
  handleScan();
  if (wm.getConfigPortalActive()) {
    wm.process();
  }
  handleJoin();
  if (millis() - lastBlinkMs >= 500) {
    lastBlinkMs = millis();
    digitalWrite(LED_PIN, !digitalRead(LED_PIN));
  }
  if (setupExitAtMs != 0 && (long)(millis() - setupExitAtMs) > 0) {
    exitSetupMode("linked and connected to the broker");
  } else if ((long)(millis() - setupUntilMs) > 0 && joinState == Join::Idle) {
    if (settingsComplete()) {
      exitSetupMode("timed out");
    } else {
      // A module without a home has nothing else to do: it keeps offering Bluetooth.
      setupUntilMs = millis() + SETUP_MODE_TIMEOUT_MS;
    }
  }
}

// Called from mqtt.loop(): only flags the command, handled in loop().
void onMqttMessage(char* topic, byte* payload, unsigned int length) {
  if (strcmp(topic, topicCommand) != 0 || length == 0) {
    return;   // an empty retained message only clears a previous command
  }
  JsonDocument doc;
  if (deserializeJson(doc, payload, length)) {
    return;
  }
  if (strcmp(doc["cmd"] | "", "unlink") == 0) {
    unlinkRequested = true;
  }
}

// The module was removed from its home: its key no longer works. It forgets it
// (keeping the Wi-Fi network) and offers Bluetooth right away to be linked again.
void handleUnlinkCommand() {
  if (!unlinkRequested) {
    return;
  }
  unlinkRequested = false;
  logLine("[CFG] removed from its home in the web app: forgetting the api key");
  settings.apiKey[0] = '\0';
  saveSettings();
  mqtt.disconnect();
  enterSetupMode(SetupReason::Unlinked, "module removed from its home");
}

// Defined here, not next to its variables: the Arduino builder puts function
// prototypes above the first function, before the types other prototypes need.
void IRAM_ATTR onPortalButton() {
  if (digitalRead(PORTAL_BUTTON_PIN) == LOW) {
    if (buttonPressedAtMs == 0) {
      buttonPressedAtMs = millis();
    }
  } else {
    if (buttonPressedAtMs != 0 && millis() - buttonPressedAtMs >= PORTAL_BUTTON_HOLD_MS) {
      buttonHoldDone = true;
    }
    buttonPressedAtMs = 0;
  }
}

void handlePortalButton() {
  // A hold that ended while loop() was blocked, or one still going on.
  bool held = buttonHoldDone;
  unsigned long pressedAt = buttonPressedAtMs;
  if (!held && pressedAt != 0 && digitalRead(PORTAL_BUTTON_PIN) == LOW &&
      millis() - pressedAt >= PORTAL_BUTTON_HOLD_MS) {
    held = true;
    buttonPressedAtMs = millis();   // one trigger per hold
  }
  if (held) {
    buttonHoldDone = false;
    enterSetupMode(SetupReason::Button, "BOOT button pressed");
  }
}

// -----------------------------------------------------------------------------
// Connectivity
// -----------------------------------------------------------------------------
// Staged retry of the saved network:
//   - every WIFI_RETRY_MS: the stuck attempt is dropped and a new one starts
//   - every 4 retries (~2 min): the Wi-Fi radio is switched off and on
//   - after WIFI_RESTART_AFTER_MS without network: the ESP32 restarts, which
//     always works (queued samples are lost, but it never stays disconnected)
void retryWiFi() {
  wifiRetries++;
  logLine("[WiFi] retry " + String(wifiRetries) + " to '" + WiFi.SSID() + "', status=" +
          String(WiFi.status()) + " reason=" + String(lastDisconnectReason));
  if (settingsComplete() && wifiLostSinceMs != 0 &&
      millis() - wifiLostSinceMs >= WIFI_RESTART_AFTER_MS &&
      (bleServer == nullptr || bleServer->getConnectedCount() == 0)) {
    logLine("[WiFi] no network for too long: restarting the module");
    delay(200);
    ESP.restart();
  }
  if (wifiRetries % 4 == 0) {
    WiFi.mode(WIFI_OFF);
    delay(200);
    WiFi.mode(WIFI_STA);
  } else {
    WiFi.disconnect(false, false);
    delay(100);
  }
  beginSavedWiFi();
}

void ensureConnectivity() {
  if (joinState == Join::Connecting) {
    return;   // linking controls the Wi-Fi right now
  }
  if (WiFi.status() != WL_CONNECTED) {
    // If the network does not come back within WIFI_SETUP_AFTER_MS it may have
    // changed: Bluetooth is offered to set the new one, while the saved one
    // keeps being retried.
    if (wifiLostSinceMs == 0) {
      wifiLostSinceMs = millis();
      logLine("[WiFi] disconnected, retrying");
    } else if (!setupMode && millis() - wifiLostSinceMs >= WIFI_SETUP_AFTER_MS) {
      enterSetupMode(SetupReason::NoWifi, "no Wi-Fi for 5 min");
    }
    // setAutoReconnect does not always recover (a router slow to come back after
    // a power cut, radio mode changes): retry explicitly. Not while the portal
    // is open, which needs the radio for the access point.
    if (!wm.getConfigPortalActive() && !scanRunning && millis() - lastWifiRetryMs >= WIFI_RETRY_MS) {
      lastWifiRetryMs = millis();
      retryWiFi();
    }
    return;
  }
  wifiRetries = 0;
  if (wifiLostSinceMs != 0) {
    wifiLostSinceMs = 0;
    logWiFi();
  }
  if (mqtt.connected()) {
    brokerLostSinceMs = 0;
    return;
  }
  if (brokerLostSinceMs == 0) {
    brokerLostSinceMs = millis();
  } else if (!setupMode && settingsComplete() && joinState == Join::Idle &&
             millis() - brokerLostSinceMs >= BROKER_SETUP_AFTER_MS) {
    enterSetupMode(SetupReason::NoBroker, "the server does not answer from this network");
  }

  // Somebody is configuring the module over Bluetooth: an attempt against an
  // unreachable server blocks loop() for seconds and would delay the scan and the
  // new configuration they are about to send. Retry once they disconnect.
  if (bleServer != nullptr && bleServer->getConnectedCount() > 0 && joinState == Join::Idle) {
    return;
  }

  unsigned long now = millis();
  if (now - lastReconnectAttemptMs < reconnectDelayMs && lastReconnectAttemptMs != 0) {
    return;
  }
  lastReconnectAttemptMs = now;

  if (mqttConnect()) {
    return;
  }

  // Bounded exponential backoff (FRS5.4 - reconnection without intervention).
  reconnectDelayMs =
      (reconnectDelayMs * 2 > RECONNECT_MAX_DELAY_MS) ? RECONNECT_MAX_DELAY_MS
                                                       : reconnectDelayMs * 2;
}

void onWiFiEvent(WiFiEvent_t event, WiFiEventInfo_t info) {
  if (event == ARDUINO_EVENT_WIFI_STA_DISCONNECTED) {
    lastDisconnectReason = info.wifi_sta_disconnected.reason;
  }
}

// -----------------------------------------------------------------------------
// Periodic sampling
// -----------------------------------------------------------------------------
// Runs regardless of the network state to keep the 60 s cadence: if the broker
// goes down, samples pile up in the offline queue instead of being dropped.
void handleSampling() {
  unsigned long now = millis();
  if (now - lastTelemetryMs < TELEMETRY_INTERVAL_MS) {
    return;
  }
  lastTelemetryMs = now;

  Measurement m;
  if (!readPzEM(m)) {
    return;
  }

  char dateTime[32] = "no time";
  if (m.epoch != 0) {
    formatIso8601(m.epoch, dateTime, sizeof(dateTime));
  }
  logLine("[PZEM] " + String(m.voltage, 1) + "V " + String(m.current, 3) + "A " +
          String(m.activePower, 1) + "W " + String(m.storedEnergy, 3) + "kWh @" +
          String(dateTime));

  if (mqtt.connected()) {
    if (publishTelemetry(m)) {
      logLine("[MQTT] telemetry published");
    } else {
#if ENABLE_OFFLINE_QUEUE
      enqueueOffline(m);
      logLine("[MQTT] publish failed, sample queued");
#endif
    }
  } else {
#if ENABLE_OFFLINE_QUEUE
    enqueueOffline(m);
    logLine("[MQTT] not connected, sample queued (" + String(queueSize) +
            "/" + String(OFFLINE_QUEUE_SIZE) + ")");
#else
    logLine("[MQTT] not connected, sample dropped");
#endif
  }
}

// -----------------------------------------------------------------------------
// Setup / Loop
// -----------------------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  delay(400);
  logLine("");
  logLine("=== EnergyMonitor ESP32 + PZEM-004T V3 fw=" FIRMWARE_VERSION " ===");

  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);
  pinMode(PORTAL_BUTTON_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(PORTAL_BUTTON_PIN), onPortalButton, CHANGE);

  // PZEM-004T V3: Modbus RTU at 9600 8N1 over UART2.
  Serial2.begin(PZEM_BAUD, SERIAL_8N1, PZEM_RX_PIN, PZEM_TX_PIN);
  logLine("[PZEM] UART2 ready at " + String(PZEM_BAUD) + " baud, addr=0x" +
          String(PZEM_ADDRESS, HEX));

  loadSettings();
  configureMqtt();
  applySettings();

  wm.addParameter(&paramHost);
  wm.addParameter(&paramPort);
  wm.addParameter(&paramDeviceId);
  wm.addParameter(&paramDeviceCode);
  wm.addParameter(&paramApiKey);
  wm.setSaveParamsCallback(onPortalSave);
  wm.setTitle("EnergyMonitor");

  WiFi.onEvent(onWiFiEvent);
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.setSleep(true);   // required while Bluetooth is active (radio coexistence)

  // date_time is NOT NULL in `measurement` and TLS needs the time to validate
  // the certificate. SNTP keeps synchronizing in the background.
  configTime(0, 0, "pool.ntp.org", "time.nist.gov", "time.google.com");

  // Saved network (from the portal or from Bluetooth on a previous boot).
  adoptDriverNetwork();
  beginSavedWiFi();
  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < WIFI_JOIN_TIMEOUT_MS) {
    delay(250);
  }
  if (WiFi.status() == WL_CONNECTED) {
    logWiFi();
    unsigned long ntpStart = millis();
    while (!clockValid() && millis() - ntpStart < 15000UL) {
      delay(250);
    }
    logLine(clockValid() ? "[NTP] time synchronized" : "[NTP] no time yet, retrying");
  } else {
    logLine("[WiFi] the saved network did not answer at boot, status=" + String(WiFi.status()) +
            " reason=" + String(lastDisconnectReason) + "; retrying every 30 s");
  }

  if (!settingsComplete()) {
    enterSetupMode(SetupReason::Unlinked, "module not linked");
  } else if (WiFi.status() != WL_CONNECTED) {
    // After a power cut the router usually takes longer than the ESP32 to come back.
    enterSetupMode(SetupReason::NoWifi, "the saved network does not answer");
    lastWifiRetryMs = millis();
  }

  // First reading right away instead of waiting 60 s after boot.
  lastTelemetryMs = millis() - TELEMETRY_INTERVAL_MS;
}

void loop() {
  handlePortalButton();
  handleUnlinkCommand();
  handleSetupMode();
  ensureConnectivity();

  if (mqtt.connected()) {
    mqtt.loop();
#if ENABLE_OFFLINE_QUEUE
    flushOfflineQueue();
#endif
    if (millis() - lastStatusMs >= STATUS_INTERVAL_MS) {
      publishStatus("online");
    }
  }

  handleSampling();
}
