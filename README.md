# Vesta Sous Vide → Home Assistant (Gizwits MQTT bridge)

Controls a Vesta sous vide cooker (Gizwits-based, product `SousVideCooker`) from
Home Assistant over MQTT. The vendor app keeps working — the bridge uses the same
Gizwits cloud API the app's backend uses: OpenAPI for login/polling/control plus
the WebSocket channel for live push updates.

## How it works

```
cooker ⇄ Gizwits cloud (OpenAPI + WebSocket) ⇄ bridge ⇄ MQTT ⇄ Home Assistant
```

- **State in**: polls `GET /app/devdata/{did}/latest` (default every 20 s) and
  listens on `wss://{host}:8880/ws/app/v1` (`login_req` / `ping` heartbeat /
  `s2c_noti`). Publishes merged state to `vesta/<did>/state`.
- **Commands out**: JSON attrs on `vesta/<did>/set` → `POST /app/control/{did}`.
  Same datapoint keys the vendor app uses (`onoff`, `set_temp_integer/decimal`,
  `set_time_hour/min`, `temp_unit`, `res1`, `dev_reset` — refused for safety).
- **HA entities** via MQTT discovery: climate (temp + on/off), timer (total
  minutes), timer start/stop buttons, temp-unit select, time-remaining sensor,
  8 alert binary sensors. `fault_code`/`fault_bits` from the `error_code`
  datapoint are exposed in the state JSON for correlation (meanings undecoded).

Protocol details (decompiled from the vendor APK): see `tooling/PROTOCOL.md`
in the dev workspace — not shipped here.

## Prerequisites

- Home Assistant with the MQTT integration + a reachable broker (e.g. Mosquitto)
- Your Vesta/Gizwits account credentials — the cooker must already be bound in
  the vendor app. Note: the app caps passwords at **16 chars**, so only the
  first 16 are your effective password (the bridge truncates the same way).
- If your account is EU-based, set `GIZ_API=https://euapi.gizwits.com`
  (default is the US host).

## Setup

```bash
cp .env.example .env
# edit .env: GIZ_USER + GIZ_PASS (or GIZ_UID + GIZ_TOKEN), MQTT_HOST, ...
```

Prebuilt images are published on every GitHub release (plus manual runs) to
both registries, `linux/amd64` + `linux/arm64`:

- `ghcr.io/fcolasuonno/vesta-sous-vide-ha-bridge:latest` (and `:vX.Y.Z` per release)
- `docker.io/fcolasuonno/vesta-sous-vide-ha-bridge:latest`

```bash
docker run -d --name vesta-bridge --restart unless-stopped --env-file .env \
  ghcr.io/fcolasuonno/vesta-sous-vide-ha-bridge:latest
```

> Maintainers: Docker Hub pushes need `DOCKERHUB_USERNAME` + `DOCKERHUB_TOKEN`
> repo secrets (GHCR uses the automatic `GITHUB_TOKEN`).

### Option A — plain Docker

```bash
docker build -t vesta-bridge .
docker run -d --name vesta-bridge --restart unless-stopped --env-file .env vesta-bridge
docker logs -f vesta-bridge   # expect: login → device → polled:/ws: lines
```

If Mosquitto runs in another container, put both on the same Docker network so
`MQTT_HOST` resolves, or point `MQTT_HOST` at the broker's IP.

### Option B — add to your existing compose stack

```yaml
services:
  vesta-bridge:
    build: /path/to/this/repo
    container_name: vesta-bridge
    env_file: /path/to/this/repo/.env
    restart: unless-stopped
    depends_on:
      - mosquitto
```

### Option C — direct Python (no Docker)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
set -a; source .env; set +a
python bridge.py            # add --once for a single poll, --no-ws to skip WebSocket
```

## Home Assistant

The `Vesta Sous Vide` device should auto-appear (Settings → Devices & Services →
MQTT) within a minute via discovery. If not: confirm HA uses the same broker,
the discovery prefix is `homeassistant`, or reload the MQTT integration.

| Entity | Type | Notes |
|---|---|---|
| Vesta Sous Vide | climate | set/current temp (5–95 °C), heat/off |
| Vesta Timer | number | total minutes 0–5999 (0 = timer off); split to hour/min pair on send |
| Vesta Timer Start / Stop | buttons | Start = countdown trigger (`res1: 0`, needs cooker on + at temp); Stop zeroes the timer |
| Vesta Temp Unit | select | °C/°F (`temp_unit`) |
| Vesta Time Remaining | sensor | minutes |
| 8× alerts (At Temperature, Cooking Finished, Low/No Water, …) | binary_sensor | fault-type ones carry `device_class: problem` |

## Troubleshooting

- `docker logs vesta-bridge` prints every `control ->` with the cloud response.
- `9005 user does not exist` → wrong username/region host. `9004 token invalid`
  → re-login (restart the bridge). `9014` → wrong `GIZ_DID`.
- **E01 on the cooker display** = water-level fault from the firmware: immerse
  between MIN/MAX with **tap water** (distilled/purified blinds the sensor).
- The bridge never sends `dev_reset` (factory reset) or `clean_model` (dormant
  wash mode) — refused by design.
