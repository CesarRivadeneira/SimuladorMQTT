
# sim_soia_hivemq.py
# Simulador de 6 señales para HiveMQ Cloud (o cualquier broker MQTT)
# Publica: nivel (1–5V), caudal gas (1–5V → m3/min), pulsos A/B, DI sensor inyección, estado A/B y QA.
#
# Config por variables de entorno (ejemplos al final del archivo).
import os, time, math, random, json, ssl
from dataclasses import dataclass
from datetime import datetime, timezone
import paho.mqtt.client as mqtt


try:
    from dotenv import load_dotenv
    load_dotenv()               # por defecto busca .env en el cwd
except Exception:
    pass

def iso_utc():
    return datetime.now(timezone.utc).isoformat()

@dataclass
class Config:
    host: str = os.getenv("MQTT_HOST", "your-cluster.s1.eu.hivemq.cloud")
    port: int = int(os.getenv("MQTT_PORT", "8883"))  # HiveMQ Cloud usa 8883 (TLS)
    user: str | None = os.getenv("MQTT_USER", None)
    pwd: str | None = os.getenv("MQTT_PASS", None)
    tls: bool = os.getenv("MQTT_TLS", "1") == "1"

    product: str = os.getenv("PRODUCT_CODE", "A1B2C3")
    devices_csv: str = os.getenv("DEVICES", "dev-001")
    period_s: float = float(os.getenv("PERIOD_S", "30"))  # baja frecuencia para no consumir cuota
    qos: int = int(os.getenv("MQTT_QOS", "0"))
    retain_stat: bool = os.getenv("RETAIN_STAT", "1") == "1"

    # Proceso / Ingeniería
    q_span_m3min: float = float(os.getenv("Q_SPAN_M3MIN", "30.0"))  # 1–5V → 0..q_span (m3/min)
    C_mgm3: float = float(os.getenv("C_MGM3", "250"))               # mg/m3 en gas
    rho_g_cm3: float = float(os.getenv("RHO_G_CM3", "0.815"))       # g/cm3 del químico
    E_A_cm3: float = float(os.getenv("E_A_CM3", "0.25"))            # embolada A (cm3/pulso)
    E_B_cm3: float = float(os.getenv("E_B_CM3", "0.25"))            # embolada B (cm3/pulso)
    tank_liters: float = float(os.getenv("TANK_LITERS", "200"))     # capacidad tanque (L)
    max_spm: float = float(os.getenv("MAX_SPM", "45"))              # límite mecánico (golpes/min)
    di_fail_prob: float = float(os.getenv("DI_FAIL_PROB", "0.02"))  # prob de FAIL por período
    mismatch_noise_pct: float = float(os.getenv("MISMATCH_NOISE_PCT", "0.03"))  # ±3%

    @property
    def devices(self) -> list[str]:
        return [d.strip() for d in self.devices_csv.split(",") if d.strip()]

class DeviceState:
    def __init__(self, device_id: str, cfg: Config):
        self.device_id = device_id
        self.active = "A"
        self.no_detect_count = 0  # cuenta pulsos sin detección de líquido
        self.pulses_total_A = 0
        self.pulses_total_B = 0
        self.tank_l = cfg.tank_liters
        # señales analógicas como recorrido suave
        self.flow_volts = 2.8 + random.uniform(-0.2, 0.2)  # inicia cerca del medio
        self.level_volts = 4.6  # tanque casi lleno
        self.phase = random.uniform(0, 2*math.pi)

def make_client(cfg: Config) -> mqtt.Client:
    client = mqtt.Client(client_id=f"sim-{int(time.time())}-{random.randint(1000,9999)}", clean_session=True)
    if cfg.user and cfg.pwd:
        client.username_pw_set(cfg.user, cfg.pwd)
    if cfg.tls:
        # Usa CA del sistema; HiveMQ Cloud requiere TLS
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
    client.connect(cfg.host, cfg.port, keepalive=60)
    client.loop_start()
    return client

def clamp(v, a, b): return max(a, min(b, v))

def map_flow_volts_to_q_m3min(volts: float, cfg: Config) -> float:
    # 1–5V => 0..q_span
    return max(0.0, ((volts - 1.0) / 4.0) * cfg.q_span_m3min)

def publish_json(client, topic, obj, qos=0, retain=False):
    client.publish(topic, json.dumps(obj, separators=(",", ":")), qos=qos, retain=retain)

def run():
    cfg = Config()
    client = make_client(cfg)
    states = {dev: DeviceState(dev, cfg) for dev in cfg.devices}

    base_prefix = f"soia/{cfg.product}"
    next_stat_time = 0.0  # para re-publicar estado cada tanto (mantener visible con retain)

    print("Simulador iniciado →", cfg.host, "devices:", cfg.devices)

    while True:
        t0 = time.time()
        ts = iso_utc()

        for dev, st in states.items():
            base = f"{base_prefix}/{dev}"

            # ------ Caudal GAS (1–5 V) ------
            st.flow_volts += random.uniform(-0.06, 0.06)
            st.flow_volts = clamp(st.flow_volts, 1.0, 5.0)
            Q_m3min = map_flow_volts_to_q_m3min(st.flow_volts, cfg)
            Q_m3h = Q_m3min * 60.0

            publish_json(client, f"{base}/tele/ai/flow", {
                "ts": ts, "volts": round(st.flow_volts, 3),
                "Q_m3min": round(Q_m3min, 3), "Q_m3h": round(Q_m3h, 1)
            }, qos=cfg.qos)

            # ------ Pulsos dosificador activo ------
            E = cfg.E_A_cm3 if st.active == "A" else cfg.E_B_cm3
            spm_teo = 0.0 if E <= 0 else (Q_m3min * cfg.C_mgm3) / (1000.0 * cfg.rho_g_cm3 * E)

            # Ruido +/- y saturación por límite mecánico 45 spm
            spm_real = spm_teo * (1.0 + random.uniform(-cfg.mismatch_noise_pct, cfg.mismatch_noise_pct))
            under_limit = False
            if spm_real > cfg.max_spm:
                spm_real = cfg.max_spm
                under_limit = True

            pulses_this_period = int(round(spm_real * (cfg.period_s / 60.0)))
            pulses_this_period = max(0, pulses_this_period)

            # ------ DI sensor de inyección ------
            inj_fail = (random.random() < cfg.di_fail_prob)  # closed = FAIL
            di_state = "closed" if inj_fail else "open"      # open=OK, closed=FAIL
            publish_json(client, f"{base}/tele/di/inj_sensor", {
                "ts": ts, "state": di_state
            }, qos=cfg.qos)

            # Contar "falta de detección de líquido" por pulso
            # Si DI está en FAIL o no hubo pulsos, el contador crece;
            # si DI está OK y hubo pulsos, se resetea.
            if inj_fail or pulses_this_period == 0:
                st.no_detect_count += pulses_this_period
            else:
                st.no_detect_count = 0

            rotated = False
            reason = None
            if st.no_detect_count >= 5:
                st.active = "B" if st.active == "A" else "A"
                st.no_detect_count = 0
                rotated = True
                reason = "sensor_no_detection_after_5_pulses"

            # Totales y publicación de pulsos A/B
            if st.active == "A":
                st.pulses_total_A += pulses_this_period
            else:
                st.pulses_total_B += pulses_this_period

            # Publico A y B para que el backend pueda graficar ambos
            publish_json(client, f"{base}/tele/pulse/doserA", {
                "ts": ts, "pulses_total": st.pulses_total_A,
                "pulses_period": pulses_this_period if st.active=="A" else 0,
                "rate_per_min": round(spm_real if st.active=="A" else 0.0, 3),
                "embolada_cm3": cfg.E_A_cm3
            }, qos=cfg.qos)

            publish_json(client, f"{base}/tele/pulse/doserB", {
                "ts": ts, "pulses_total": st.pulses_total_B,
                "pulses_period": pulses_this_period if st.active=="B" else 0,
                "rate_per_min": round(spm_real if st.active=="B" else 0.0, 3),
                "embolada_cm3": cfg.E_B_cm3
            }, qos=cfg.qos)

            # ------ QA: coherencia y límite ------
            mismatch_pct = 0.0 if spm_teo == 0 else round((abs(spm_real - spm_teo) / spm_teo) * 100.0, 1)
            publish_json(client, f"{base}/tele/qa/dosing_check", {
                "ts": ts,
                "spm_teo": round(spm_teo, 3),
                "spm_real": round(spm_real, 3),
                "mismatch_pct": mismatch_pct,
                "under_dosing_due_to_limit": under_limit
            }, qos=cfg.qos)

            # ------ Nivel de tanque (1–5 V) estimado por consumo ------
            consumed_cm3 = pulses_this_period * E
            st.tank_l = max(0.0, st.tank_l - consumed_cm3 / 1000.0)
            level_pct = 100.0 * (st.tank_l / cfg.tank_liters if cfg.tank_liters > 0 else 0.0)
            st.level_volts = 1.0 + 4.0 * clamp(level_pct, 0.0, 100.0) / 100.0

            publish_json(client, f"{base}/tele/ai/level", {
                "ts": ts, "volts": round(st.level_volts, 3),
                "percent": round(level_pct, 2), "tank_l": round(st.tank_l, 3)
            }, qos=cfg.qos)

            # ------ Estado activo/rotación (retain para que el dashboard lo vea siempre) ------
            now = time.time()
            if rotated or now >= next_stat_time:
                publish_json(client, f"{base}/stat/active_doser", {
                    "ts": ts, "active": st.active, "reason": reason
                }, qos=cfg.qos, retain=cfg.retain_stat)

        # Re-publicar estado cada ~5 minutos para mantenerlo fresco si usás retain
        if time.time() - next_stat_time >= 300:
            next_stat_time = time.time()

        # Ritmo de publicación estable
        dt = time.time() - t0
        sleep_s = max(0.0, cfg.period_s - dt)
        if sleep_s > 0:
            time.sleep(sleep_s)

if __name__ == "__main__":
    """
    Ejemplo de uso (Linux/macOS/WSL/PowerShell):
      export MQTT_HOST='xxxxxxxxxxxx.s1.eu.hivemq.cloud'
      export MQTT_PORT=8883
      export MQTT_USER='tu_usuario'
      export MQTT_PASS='tu_password'
      export PRODUCT_CODE='PROD123'
      export DEVICES='dev-001,dev-002'   # opcional, por defecto dev-001
      export Q_SPAN_M3MIN=30.0           # 1–5V → 0..30 m3/min
      export C_MGM3=250
      export RHO_G_CM3=0.815
      export E_A_CM3=0.25
      export E_B_CM3=0.25
      export TANK_LITERS=200
      export MAX_SPM=45
      export PERIOD_S=12

      pip install paho-mqtt
      python sim_soia_hivemq.py
    """
    run()
