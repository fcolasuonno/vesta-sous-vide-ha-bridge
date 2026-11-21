"""Vesta Sous Vide -> Home Assistant MQTT bridge (Gizwits OpenAPI polling).

Protocol source: tooling/PROTOCOL.md (decompiled Vesta 3.8.1, product SousVideCooker).
Keeps the vendor app working: uses the same cloud OpenAPI the app's backend uses.

State topic:  vesta/<did>/state   (JSON with data + alerts + derived fields)
Command topic: vesta/<did>/set    (JSON attrs, e.g. {"onoff": true},
                 {"set_temp_integer": 60, "set_temp_decimal": 0},
                 {"set_time_hour": 2, "set_time_min": 30}, {"temp_unit": 0})

Auth: either GIZ_USER+GIZ_PASS (login each start) or GIZ_UID+GIZ_TOKEN directly.
Device: GIZ_DID or first bound device with our product key.
"""
import json
import os
import sys
import threading
import time
import traceback
import urllib.request
import urllib.error

import local as lanmod

APPID = os.getenv("GIZ_APPID", "9a6d8b3d46a246fca38dea306f98e19e")
API = os.getenv("GIZ_API", "https://usapi.gizwits.com").rstrip("/")
PRODUCT_KEY = "3db0c479df0a4df78948a1771489f493"
# Alert bits live FLAT inside the /latest attr dict (0/1 ints), not in a
# separate "alerts" object as the WS API uses. Lowest-priority last, like the app.
ALERT_KEYS = ("cooking_finish", "loss_power", "low_water_level", "no_water",
              "water_hated", "work_alert", "not_working_properly", "word_hour100")
POLL = int(os.getenv("POLL_INTERVAL", "20"))
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "")
MQTT_PASS = os.getenv("MQTT_PASS", "")
DISCOVERY_PREFIX = os.getenv("DISCOVERY_PREFIX", "homeassistant")
# Local mode: --local flag or LOCAL=1 env (same effect; env suits compose
# files where overriding command: is undesirable).


def api(method, path, token=None, body=None):
    req = urllib.request.Request(
        API + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={
            "X-Gizwits-Application-Id": APPID,
            "Content-Type": "application/json",
            **({"X-Gizwits-User-token": token} if token else {}),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        detail = e.read().decode()[:500]
        # NOTE: RuntimeError, NOT SystemExit — api() is called from MQTT/WS
        # worker threads where SystemExit would silently kill the thread
        # (and a dead MQTT loop looks alive because the main thread keeps
        # printing "polled:"). Uncaught in main, it still exits -> docker restarts.
        raise RuntimeError(f"API {method} {path} -> HTTP {e.code}: {detail}")


def login():
    if os.getenv("GIZ_TOKEN") and os.getenv("GIZ_UID"):
        return os.getenv("GIZ_UID"), os.getenv("GIZ_TOKEN")
    user, pw = os.getenv("GIZ_USER", ""), os.getenv("GIZ_PASS", "")
    if not user or not pw:
        sys.exit("Set GIZ_USER+GIZ_PASS or GIZ_UID+GIZ_TOKEN in .env (see .env.example)")
    if len(pw) > 16:
        # The Vesta app caps password fields at maxLength="16"
        # (activity_user_login.xml, activity_user_signin.xml), so the
        # account password is effectively the first 16 chars.
        print("NOTE: truncating password to first 16 chars to match the Vesta app")
        pw = pw[:16]
    r = api("POST", "/app/login", body={"username": user, "password": pw, "lang": "en"})
    return r["uid"], r["token"]


def pick_device(token):
    did = os.getenv("GIZ_DID", "")
    r = None
    for path in ("/app/bindings", "/app/devices"):
        try:
            r = api("GET", path, token)
            break
        except RuntimeError:
            continue
    if r is None:
        sys.exit("Could not list bound devices (tried /app/bindings and /app/devices).")
    devs = r.get("devices", r if isinstance(r, list) else [])
    cands = [d for d in devs if d.get("product_key") == PRODUCT_KEY] or devs
    if did:
        match = [d for d in devs if d.get("did") == did]
        d = match[0] if match else {"did": did}
        return d["did"], d.get("host"), d.get("wss_port", 8880)
    if not cands:
        sys.exit("No bound devices found. Bind the cooker in the Vesta app first.")
    d = cands[0]
    print(f"Using device did={d.get('did')} mac={d.get('mac')} name={d.get('dev_alias') or d.get('remark')}")
    return d["did"], d.get("host"), d.get("wss_port", 8880)


def get_passcode(token, did):
    """LAN passcode: explicit env wins, else the cloud bindings entry."""
    if os.getenv("GIZ_PASSCODE"):
        return os.getenv("GIZ_PASSCODE")
    for path in ("/app/bindings", "/app/devices"):
        try:
            r = api("GET", path, token)
        except RuntimeError:
            continue
        devs = r.get("devices", r if isinstance(r, list) else [])
        for d in devs:
            if d.get("did") == did and d.get("passcode"):
                return d["passcode"]
    return None


def ws_loop(uid, token, did, ws_host, ws_port, on_attrs):
    """Gizwits WS live channel (mirrors gizwits-wechat-js-sdk): login_req with
    p0_type attrs_v4, {"cmd":"ping"} heartbeat, s2c_noti pushes. Reconnects
    forever; OpenAPI polling remains as fallback."""
    import time
    import websocket  # websocket-client
    url = f"wss://{ws_host}:{ws_port}/ws/app/v1"
    backoff = 5
    while True:
        try:
            print(f"ws: connecting {url}")
            ws = websocket.create_connection(url, timeout=20)
            ws.send(json.dumps({"cmd": "login_req", "data": {
                "appid": APPID, "uid": uid, "token": token,
                "p0_type": "attrs_v4", "heartbeat_interval": 60,
                "auto_subscribe": True}}))
            ws.settimeout(50)
            last_ping = time.monotonic()
            while True:
                try:
                    raw = ws.recv()
                except Exception:
                    # recv timeout -> heartbeat, like the JS SDK's _startPing
                    if time.monotonic() - last_ping >= 50:
                        ws.send(json.dumps({"cmd": "ping"}))
                        last_ping = time.monotonic()
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                cmd = msg.get("cmd")
                if cmd == "login_res":
                    if msg.get("data", {}).get("success"):
                        print("ws: login ok, subscribing", did)
                        ws.send(json.dumps({"cmd": "subscribe_req",
                                            "data": [{"did": did}]}))
                        backoff = 5
                    else:
                        print("ws: login refused, will retry")
                        break
                elif cmd == "s2c_noti" and msg.get("data", {}).get("did") == did:
                    print("ws: noti ->", json.dumps(msg["data"].get("attrs", {}))[:200])
                    on_attrs(msg["data"].get("attrs", {}) or {})
                elif cmd == "s2c_online_status" and msg.get("data", {}).get("did") == did:
                    print("ws: device online =", msg["data"].get("online"))
                # pong / subscribe_res / s2c_invalid_msg: nothing to do
        except Exception as e:
            print(f"ws: {type(e).__name__}: {str(e)[:150]} — retry in {backoff}s")
        time.sleep(backoff)
        backoff = min(backoff * 2, 300)


def to_float(i, d):
    try:
        return round(float(i) + float(d) / 10.0, 1)
    except (TypeError, ValueError):
        return None


def decode_faults(value):
    """Decode the 2-byte error_code datapoint into set-bit indices.

    REST /latest gives a hex string ("0100"), WS s2c_noti gives an int list
    ([1, 0]). Interpreted little-endian.
    STATUS: bit meanings UNKNOWN. Bit 0 is permanently set — observed during
    a real E01 episode, with a clear display, AND after a 10s power cycle —
    so it is neither the live E01 indicator nor latched history. The real E01
    signal likely lives elsewhere (transient no_water/low_water alert, or a
    display-only firmware code). Labels stay neutral (bit indices) until a
    display fault can be correlated live against this field.
    """
    try:
        if isinstance(value, str):
            raw = bytes.fromhex(value)
            b0, b1 = (list(raw) + [0, 0])[:2]
        elif isinstance(value, (list, tuple)):
            b0, b1 = (list(value) + [0, 0])[:2]
        elif isinstance(value, int):
            b0, b1 = value & 0xFF, (value >> 8) & 0xFF
        else:
            return 0, []
        code = (int(b0) & 0xFF) | ((int(b1) & 0xFF) << 8)
        return code, [i for i in range(16) if code & (1 << i)]
    except (ValueError, TypeError):
        return 0, []
    try:
        if isinstance(value, str):
            raw = bytes.fromhex(value)
            b0, b1 = (list(raw) + [0, 0])[:2]
        elif isinstance(value, (list, tuple)):
            b0, b1 = (list(value) + [0, 0])[:2]
        elif isinstance(value, int):
            b0, b1 = value & 0xFF, (value >> 8) & 0xFF
        else:
            return 0, []
        code = (int(b0) & 0xFF) | ((int(b1) & 0xFF) << 8)
        return code, [f"E{i + 1:02d}" for i in range(16) if code & (1 << i)]
    except (ValueError, TypeError):
        return 0, []


def derive(payload):
    """Mirror DeviceStatus.java: combine data+alerts into HA-friendly state."""
    attr = payload.get("attr", {}) or {}
    # Some API shapes nest under 'attrs'
    if not attr and isinstance(payload.get("attrs"), dict):
        attr = payload["attrs"]
    ws_alerts = payload.get("alerts", {}) or {}
    flat_alerts = {k: True for k in ALERT_KEYS if attr.get(k)}
    alerts = {**{k: bool(v) for k, v in ws_alerts.items() if v}, **flat_alerts}
    unit = "°C" if attr.get("temp_unit", 0) == 0 else "°F"
    fault_code, fault_bits = decode_faults(attr.get("error_code"))
    state = {
        "on": bool(attr.get("onoff", False)),
        "unit": unit,
        "set_temp": to_float(attr.get("set_temp_integer"), attr.get("set_temp_decimal")),
        "current_temp": to_float(attr.get("real_temp_integer"), attr.get("real_temp_decimal")),
        "set_time_min_total": (attr.get("set_time_hour") or 0) * 60 + (attr.get("set_time_min") or 0),
        "remaining_min_total": (attr.get("remaining_time_hour") or 0) * 60 + (attr.get("remaining_time_min") or 0),
        "running_min_total": (attr.get("runnning_time_hour") or 0) * 60 + (attr.get("runnning_time_min") or 0),
        "res1": attr.get("res1"),
        "res2": attr.get("res2"),
        "alerts": {k: bool(v) for k, v in alerts.items() if v},
        "fault_code": fault_code,
        "fault_bits": fault_bits,
        "raw": attr,
    }
    # heated/at-temp heuristic from DeviceStatus.java (res1==0 && res2==0)
    state["heating"] = bool(state["on"] and attr.get("res1") == 0 and attr.get("res2") == 0)
    return state


def load_recipes(path="recipes.yaml"):
    """Load the cookbook converted from the app guide. [] if absent."""
    try:
        import yaml
    except ImportError:
        print("recipes: pyyaml not installed, cookbook disabled")
        return []
    try:
        with open(path) as f:
            recs = yaml.safe_load(f) or []
        print(f"recipes: loaded {len(recs)} entries from {path}")
        return recs
    except FileNotFoundError:
        print(f"recipes: {path} not found, cookbook disabled")
        return []


def recipe_options(recipes, protein=None, cut=None):
    """Cascading options: proteins A-Z; cuts A-Z for protein; doneness names
    for cut (temp order — see recipe_doneness for display labels)."""
    prots, cuts, cooks = [], [], []
    for r in recipes:
        if r["protein"] not in prots:
            prots.append(r["protein"])
        if protein and r["protein"] == protein and r["cut"] not in cuts:
            cuts.append(r["cut"])
        if protein and cut and r["protein"] == protein and r["cut"] == cut \
                and r["doneness"] not in cooks:
            cooks.append(r["doneness"])
    return sorted(prots), sorted(cuts), cooks


def doneness_label(entry):
    return f"{entry['doneness']} · {entry['temp_c']}°C · {entry['time_min']}m"


def fmt_mins(m):
    h, mm = divmod(int(m or 0), 60)
    if h and mm:
        return f"{h}h {mm}m"
    return f"{h}h" if h else f"{mm}m"


def range_label(entry):
    lo, hi, t0, t1 = entry["temp_lo_c"], entry["temp_hi_c"], entry["time_min"], entry["time_max_min"]
    temp = f"{lo}°C" if lo == hi else f"{lo}-{hi}°C"
    time = fmt_mins(t0) if t0 == t1 else f"{fmt_mins(t0)}-{fmt_mins(t1)}"
    return f"{entry['doneness']} · {temp} · {time}"


def recipe_doneness(recipes, protein, cut):
    """(label, doneness) pairs for a cut, sorted by temp ascending.
    Labels carry the guide range: 'Medium-Rare · 54-57°C · 45m-4h'."""
    rows = [(r["temp_lo_c"], range_label(r), r["doneness"])
            for r in recipes if r["protein"] == protein and r["cut"] == cut]
    rows.sort(key=lambda t: (t[0] is None, t[0]))
    return [(label, name) for _, label, name in rows]


BONE_OPTIONS = ["N/A", "Boneless", "Bone-in"]


def resolve_recipe(recipes, protein, cut, doneness):
    for r in recipes:
        if r["protein"] == protein and r["cut"] == cut and r["doneness"] == doneness:
            return r
    return None


def split_temp(temp_c, use_f):
    """Split a °C float into (integer, tenths) in the cooker's active unit."""
    t = temp_c * 9.0 / 5.0 + 32.0 if use_f else temp_c
    t = min(max(t, 41 if use_f else 5), 203 if use_f else 95)
    i = int(t)
    return i, int(round((t - i) * 10))


# Thickness options (cm) offered like Sage's flow. The guide assumes ~1 inch
# (2.5 cm); its own rule (+30..40 min per 1/2 inch over) is applied at midpoint.
THICKNESSES = ["1.3 cm", "1.9 cm", "2.5 cm", "3.2 cm", "3.8 cm", "5.0 cm"]
THICK_BASE_CM = 2.5
THICK_STEP_CM = 1.27
THICK_EXTRA_MIN = 35
# Frozen rule of thumb (not guide data — clearly an estimate), applied only
# to proteins where starting frozen is sane. Eggs/desserts/sauces/grains etc.
# always cook from the tabulated values.
FROZEN_FACTOR = 1.5
FROZEN_PROTEINS = {"Beef", "Pork", "Poultry", "Lamb", "Fish and Seafood"}


def adjust_time(base_min, entry, frozen, thickness, bone):
    """Fresh/frozen + thickness + bone adjustments from the entry's own rules:
    - thickness extra only when the guide note carries a thickness rule
      (per/over/add parsed by the converter);
    - frozen x1.5 only for FROZEN_PROTEINS (never eggs/desserts/sauces);
    - bone-in doubles or adds minutes only when the entry says so.
    Returns total minutes (int)."""
    total = base_min
    per = entry.get("thick_per_in")
    if per:
        try:
            cm = float(thickness.split()[0])
        except (ValueError, IndexError, AttributeError):
            cm = THICK_BASE_CM
        over_cm = (entry.get("thick_over_in") or 1.0) * 2.54
        per_cm = per * 2.54
        total += max(0, int((cm - over_cm) / per_cm + 0.5)) * (entry.get("thick_add_min") or 0)
    if frozen == "Frozen" and entry.get("protein") in FROZEN_PROTEINS:
        total *= FROZEN_FACTOR
    if bone == "Bone-in":
        if entry.get("bone_double"):
            total *= 2
        total += entry.get("bone_add_min") or 0
    return int(round(total))


def main():
    import paho.mqtt.client as mqtt  # lazy import so --help works without deps

    uid, token = login()
    print(f"Logged in uid={uid}")
    did, ws_host, ws_port = pick_device(token)
    state_topic = f"vesta/{did}/state"
    set_topic = f"vesta/{did}/set"

    # --- LAN mode: GAgent session primary, cloud fallback ---
    use_local = "--local" in sys.argv or os.getenv("LOCAL", "").lower() in ("1", "true", "yes")
    lan = {"session": None, "ip": None}
    lan_lock = threading.Lock()
    passcode = get_passcode(token, did) if use_local else None
    if use_local and not passcode:
        print("lan: no passcode found, staying cloud-only")
        use_local = False

    def lan_ensure():
        if lan["session"] is not None:
            return True
        # Always auto-discover: no IP configuration needed (requires a
        # broadcast-capable network: host/macvlan, not plain docker bridge).
        found = lanmod.discover(did=did)
        if not found:
            return False
        ip = found[0]["ip"]
        lan["ip"] = ip
        sess = lanmod.GAgentSession(ip, passcode)
        sess.connect()
        lan["session"] = sess
        print(f"lan: connected {ip}")
        return True

    def lan_close():
        try:
            if lan["session"] is not None:
                lan["session"].close()
        except Exception:
            pass
        lan["session"] = None

    def control(attrs, where="control"):
        """Write via LAN when up, else cloud. Never raises."""
        if use_local:
            try:
                lan_ensure()
                with lan_lock:
                    lan["session"].write(attrs)
                print(f"{where} -> LAN", attrs)
                return
            except Exception:
                traceback.print_exc()
                lan_close()
        r = api("POST", f"/app/control/{did}", token, {"attrs": attrs})
        print(f"{where} ->", attrs, "resp:", json.dumps(r)[:200])

    mc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USER:
        mc.username_pw_set(MQTT_USER, MQTT_PASS)
    mc.will_set(state_topic + "/availability", "offline", retain=True)
    last_raw = {}  # latest full attr dict; paired writes + WS partial merges
    raw_lock = threading.Lock()

    def publish_state(source):
        state = derive({"attr": dict(last_raw)})
        # recipe selection snapshot (callers hold raw_lock)
        if recipes:
            snap = dict(recipe_sel)
            entry = resolve_recipe(
                recipes, snap["protein"], snap["cut"], snap["doneness"])
            # Full detail snapshot so HA can show notes (thickness guidance
            # etc.), temp and time without another lookup.
            snap["doneness_label"] = range_label(entry) if entry else snap["doneness"]
            snap["target_temp"] = snap.get("target")
            snap["info"] = entry.get("info", "") if entry else ""
            snap["temp_c"] = entry.get("temp_c") if entry else None
            snap["time_min"] = entry.get("time_min") if entry else None
            snap["cook_time_min"] = adjust_time(
                snap["time_min"] or 0, entry,
                snap.get("fresh", "Fresh"), snap.get("thickness", "2.5 cm"),
                snap.get("bone", "N/A")) if entry else None
            state["recipe"] = snap
        else:
            state["recipe"] = None
        mc.publish(state_topic, json.dumps(state), retain=True)
        print(f"{source}:", json.dumps(state)[:220])

    def on_ws_attrs(partial):
        # s2c_noti carries only changed attrs — merge into the last full
        # picture before deriving, otherwise HA would see nulls.
        with raw_lock:
            last_raw.update(partial)
            publish_state("ws")

    def on_set(client, userdata, msg):
        try:
            attrs = json.loads(msg.payload.decode())
        except ValueError:
            print("Ignoring non-JSON command")
            return
        if not isinstance(attrs, dict):
            print("Ignoring non-object command")
            return
        try:
            handle_set(attrs)
        except Exception:
            # Never let a handler bug silently kill the MQTT loop thread.
            traceback.print_exc()

    def handle_set(attrs):
        if attrs.get("cook_recipe"):
            # Worker thread: 3 sequential cloud calls + sleeps must never
            # block the MQTT loop (a stuck loop looks alive — main thread
            # keeps printing "polled:" — while nothing is received).
            threading.Thread(target=do_cook, daemon=True).start()
            return
        # clean_model is deliberately NOT allowed (user scope decision) even
        # though the cloud marks it writable — the app never touches it.
        allowed = {"onoff", "dev_reset", "set_temp_integer",
                   "set_temp_decimal", "temp_unit", "set_time_hour",
                   "set_time_min", "res1", "res2"}
        attrs = {k: v for k, v in attrs.items() if k in allowed}
        if not attrs:
            return
        if attrs.get("dev_reset"):
            print("REFUSING dev_reset via MQTT (factory reset). Send via app if really needed.")
            return
        # The firmware ignores lone set_time_hour / set_time_min writes —
        # the app always sends both together (DeviceContentActivity.handleTime).
        # Merge with the last polled values so single-key writes stick.
        if "set_time_hour" in attrs or "set_time_min" in attrs:
            with raw_lock:
                paired = {"set_time_hour": int(last_raw.get("set_time_hour", 0)),
                          "set_time_min": int(last_raw.get("set_time_min", 0))}
            paired.update({k: int(v) for k, v in attrs.items()
                           if k in ("set_time_hour", "set_time_min")})
            attrs = {k: v for k, v in attrs.items() if k not in paired} | paired
        control(attrs)

    mc.message_callback_add(set_topic, on_set)
    node = f"vesta_{did[-6:]}"

    # --- Cookbook (cascading selects -> cook button stages + starts heat) ---
    recipes = load_recipes()
    recipe_base = f"vesta/{did}/recipe"
    recipe_sel = {}
    if recipes:
        prots, cuts, _ = recipe_options(recipes)
        recipe_sel = {"protein": prots[0], "cut": None, "doneness": None,
                      "fresh": "Fresh", "thickness": "2.5 cm", "bone": "N/A",
                      "target": None}
        _, cuts, _ = recipe_options(recipes, prots[0])
        recipe_sel["cut"] = cuts[0]
        first = resolve_recipe(
            recipes, prots[0], cuts[0],
            recipe_doneness(recipes, prots[0], cuts[0])[0][1])
        recipe_sel["doneness"] = first["doneness"]
        recipe_sel["target"] = first["temp_c"]

    def do_cook():
        """Stage resolved recipe temp+time (paired, like the app) and heat on.
        Timer intentionally NOT started — user presses Timer Start at-temp."""
        try:
            with raw_lock:
                sel = dict(recipe_sel)
                use_f = last_raw.get("temp_unit", 0) == 1
            entry = resolve_recipe(recipes, sel["protein"], sel["cut"], sel["doneness"])
            if not entry:
                print("cook: no recipe resolved for", sel)
                return
            ti, td = split_temp(sel.get("target", entry["temp_c"]) or entry["temp_c"], use_f)
            total_min = adjust_time(entry["time_min"], entry,
                                    sel.get("fresh", "Fresh"),
                                    sel.get("thickness", "2.5 cm"),
                                    sel.get("bone", "N/A"))
            th, tm = divmod(total_min, 60)
            print(f"cook: {sel['protein']} / {sel['cut']} / {sel['doneness']}"
                  f" [{sel.get('fresh')}, {sel.get('thickness')}, {sel.get('bone')}]"
                  f" -> {ti}.{td}{'F' if use_f else 'C'}, {th}h{tm:02d}m")
            control({"set_temp_integer": ti, "set_temp_decimal": td}, "cook")
            time.sleep(1)
            control({"set_time_hour": th, "set_time_min": tm}, "cook")
            time.sleep(1)
            control({"onoff": True}, "cook")
        except Exception:
            traceback.print_exc()

    def publish_recipe_selects(client):
        if not recipes:
            return
        prots, cuts, _ = recipe_options(
            recipes, recipe_sel["protein"], recipe_sel["cut"])
        dons = recipe_doneness(recipes, recipe_sel["protein"], recipe_sel["cut"])
        cur = resolve_recipe(recipes, recipe_sel["protein"],
                             recipe_sel["cut"], recipe_sel["doneness"])
        # Fresh/thickness/bone only exist where they mean something (eggs show
        # none of them): frozen needs a freezable protein, thickness/bone need
        # a rule on the entry. Inapplicable selects are DELETED so
        # HA drops the entities instead of showing dead controls.
        show_fresh = recipe_sel["protein"] in FROZEN_PROTEINS
        show_thick = bool(cur and cur.get("thick_per_in"))
        show_bone = bool(cur and (cur.get("bone_double") or cur.get("bone_add_min")))
        for level, opts, field, step, show in (
                ("protein", prots, "protein", 1, True),
                ("cut", cuts, "cut", 2, True),
                ("doneness", [label for label, _ in dons], "doneness_label", 3, True),
                ("fresh", ["Fresh", "Frozen"], "fresh", 4, show_fresh),
                ("thickness", THICKNESSES, "thickness", 5, show_thick),
                ("bone", BONE_OPTIONS, "bone", 6, show_bone)):
            topic = f"{DISCOVERY_PREFIX}/select/{node}_recipe_{level}/config"
            if not show:
                client.publish(topic, b"", retain=True)
                continue
            client.publish(
                f"{DISCOVERY_PREFIX}/select/{node}_recipe_{level}/config", json.dumps({
                    # numbered prefix so HA sorts Protein -> Cut -> Doneness
                    "name": f"Vesta Recipe {step} - {level.capitalize()}",
                    "unique_id": f"{node}_recipe_{level}",
                    "state_topic": state_topic,
                    "value_template": f"{{{{ value_json.recipe.{field} }}}}",
                    "command_topic": f"{recipe_base}/{level}/set",
                    "options": opts,
                    "availability_topic": state_topic + "/availability",
                    "device": {"identifiers": [did], "name": "Vesta Sous Vide",
                               "model": "SousVideCooker"},
                }), retain=True)
        client.publish(
            f"{DISCOVERY_PREFIX}/button/{node}_recipe_cook/config", json.dumps({
                "name": "Vesta Recipe Cook", "unique_id": f"{node}_recipe_cook",
                "command_topic": set_topic, "payload_press": '{"cook_recipe": true}',
                "availability_topic": state_topic + "/availability",
                "device": {"identifiers": [did], "name": "Vesta Sous Vide",
                           "model": "SousVideCooker"},
            }), retain=True)
        # Cook target temp: bounded by the current selection's guide range,
        # defaults to its midpoint. Cook stages this, not the midpoint.
        cur = resolve_recipe(recipes, recipe_sel["protein"],
                             recipe_sel["cut"], recipe_sel["doneness"])
        if cur:
            client.publish(
                f"{DISCOVERY_PREFIX}/number/{node}_recipe_target/config", json.dumps({
                    "name": "Vesta Recipe Target Temp", "unique_id": f"{node}_recipe_target",
                    "state_topic": state_topic,
                    "value_template": "{{ value_json.recipe.target_temp }}",
                    "command_topic": f"{recipe_base}/target/set",
                    "min": cur["temp_lo_c"], "max": cur["temp_hi_c"],
                    "step": 0.1, "mode": "box", "unit_of_measurement": "°C",
                    "availability_topic": state_topic + "/availability",
                    "device": {"identifiers": [did], "name": "Vesta Sous Vide",
                               "model": "SousVideCooker"},
                }), retain=True)

    def on_recipe_select(client, userdata, msg):
        # topic vesta/<did>/recipe/<protein|cut|doneness>/set, raw option string
        try:
            level = msg.topic.rsplit("/", 2)[-2]
            choice = msg.payload.decode().strip()
        except Exception:
            traceback.print_exc()
            return
        with raw_lock:
            try:
                prots, cuts, _ = recipe_options(
                    recipes, recipe_sel["protein"], recipe_sel["cut"])
                dons = dict(recipe_doneness(
                    recipes, recipe_sel["protein"], recipe_sel["cut"]))
                if level == "protein" and choice in prots:
                    recipe_sel["protein"] = choice
                    _, ncuts, _ = recipe_options(recipes, choice)
                    recipe_sel["cut"] = ncuts[0]
                    first = resolve_recipe(
                        recipes, choice, ncuts[0],
                        recipe_doneness(recipes, choice, ncuts[0])[0][1])
                    recipe_sel["doneness"] = first["doneness"]
                    recipe_sel["target"] = first["temp_c"]
                elif level == "cut" and choice in cuts:
                    recipe_sel["cut"] = choice
                    first = resolve_recipe(
                        recipes, recipe_sel["protein"], choice,
                        recipe_doneness(recipes, recipe_sel["protein"], choice)[0][1])
                    recipe_sel["doneness"] = first["doneness"]
                    recipe_sel["target"] = first["temp_c"]
                elif level == "doneness" and choice in dons:
                    recipe_sel["doneness"] = dons[choice]
                    entry = resolve_recipe(recipes, recipe_sel["protein"],
                                           recipe_sel["cut"], dons[choice])
                    recipe_sel["target"] = entry["temp_c"] if entry else None
                elif level == "fresh" and choice in ("Fresh", "Frozen"):
                    recipe_sel["fresh"] = choice
                elif level == "thickness" and choice in THICKNESSES:
                    recipe_sel["thickness"] = choice
                elif level == "bone" and choice in BONE_OPTIONS:
                    recipe_sel["bone"] = choice
                elif level == "target":
                    try:
                        recipe_sel["target"] = round(float(choice), 1)
                    except (ValueError, TypeError):
                        print(f"recipe: ignoring invalid target={choice!r}")
                        return
                else:
                    print(f"recipe: ignoring invalid {level}={choice!r}")
                    return
                print("recipe: now", recipe_sel)
                publish_recipe_selects(client)
                publish_state("recipe")
            except Exception:
                # Never let a handler bug silently kill the MQTT loop thread.
                traceback.print_exc()

    mc.message_callback_add(f"{recipe_base}/+/set", on_recipe_select)

    def publish_discovery(client):
        # HA MQTT discovery: climate + key sensors (retained so HA picks
        # them up on restart). Must run AFTER connect — pre-connect
        # publishes are silently dropped, which is why HA saw nothing.
        publish_recipe_selects(client)
        client.publish(f"{DISCOVERY_PREFIX}/climate/{node}/config", json.dumps({
            "name": "Vesta Sous Vide",
            "unique_id": node + "_climate",
            "modes": ["off", "heat"],
            "mode_state_topic": state_topic,
            "mode_state_template": "{% if value_json.on %}heat{% else %}off{% endif %}",
            "mode_command_topic": set_topic,
            "mode_command_template": '{"onoff": {{ 1 if value == "heat" else 0 }}}',
            "current_temperature_topic": state_topic,
            "current_temperature_template": "{{ value_json.current_temp }}",
            "temperature_state_topic": state_topic,
            "temperature_state_template": "{{ value_json.set_temp }}",
            "temperature_command_topic": set_topic,
            # split float setpoint into integer+decimal attrs like the app does
            "temperature_command_template": '{"set_temp_integer": {{ value | int }}, "set_temp_decimal": {{ ((value - (value | int)) * 10) | round | int }}}',
            "min_temp": 5, "max_temp": 95, "temp_step": 0.1,
            "availability_topic": state_topic + "/availability",
            "device": {"identifiers": [did], "name": "Vesta Sous Vide", "model": "SousVideCooker"},
        }), retain=True)
        client.publish(f"{DISCOVERY_PREFIX}/sensor/{node}_remaining/config", json.dumps({
            "name": "Vesta Time Remaining", "unique_id": node + "_remaining",
            "state_topic": state_topic, "value_template": "{{ value_json.remaining_min_total }}",
            "unit_of_measurement": "min", "device": {"identifiers": [did]},
        }), retain=True)
        avail = state_topic + "/availability"
        dev = {"identifiers": [did], "name": "Vesta Sous Vide", "model": "SousVideCooker"}
        # Selected recipe notes (thickness guidance etc.); falls back to the
        # cut + doneness label when the guide has no note for an entry.
        # Temp/time ride along as attributes.
        client.publish(f"{DISCOVERY_PREFIX}/sensor/{node}_recipe_notes/config", json.dumps({
            "name": "Vesta Recipe Notes", "unique_id": f"{node}_recipe_notes",
            "state_topic": state_topic,
            "value_template": "{% if value_json.recipe.info %}{{ value_json.recipe.info }}{% else %}{{ value_json.recipe.cut }} - {{ value_json.recipe.doneness_label }}{% endif %}",
            "json_attributes_topic": state_topic,
            "json_attributes_template": '{"temperature_c": {{ value_json.recipe.temp_c | default(0, true) }}, "time_min": {{ value_json.recipe.time_min | default(0, true) }}, "cut": "{{ value_json.recipe.cut }}", "doneness": "{{ value_json.recipe.doneness }}"}',
            "availability_topic": avail, "device": dev,
        }), retain=True)
        # Timer as ONE control: total minutes (0-5999). Split into the
        # hour/min pair the firmware demands, like the app does.
        client.publish(f"{DISCOVERY_PREFIX}/number/{node}_timer/config", json.dumps({
            "name": "Vesta Timer", "unique_id": f"{node}_timer",
            "state_topic": state_topic,
            "value_template": "{{ value_json.set_time_min_total }}",
            "command_topic": set_topic,
            "command_template": '{"set_time_hour": {{ (value | int) // 60 }}, "set_time_min": {{ (value | int) % 60 }}}',
            "min": 0, "max": 5999, "step": 1, "mode": "box",
            "unit_of_measurement": "min",
            "availability_topic": avail, "device": dev,
        }), retain=True)
        # Temperature unit (temp_unit: 0 = C, 1 = F, like the app)
        # Use the derived top-level `unit` field, not `raw.temp_unit`. A
        # missing/unexpected nested value makes the template raise; HA then
        # feeds the raw JSON payload to the select -> "Invalid option ...
        # '{"on": false, "unit": "°C", ...}'". Guard the render so it can
        # only ever emit one of the declared options.
        client.publish(f"{DISCOVERY_PREFIX}/select/{node}_temp_unit/config", json.dumps({
            "name": "Vesta Temp Unit", "unique_id": f"{node}_temp_unit",
            "state_topic": state_topic,
            "value_template": "{% if value_json is mapping and value_json.unit in ['\u00b0C', '\u00b0F'] %}{{ value_json.unit }}{% else %}\u00b0C{% endif %}",
            "command_topic": set_topic,
            "command_template": "{% if value == '\u00b0F' %}{\"temp_unit\": 1}{% else %}{\"temp_unit\": 0}{% endif %}",
            "options": ["\u00b0C", "\u00b0F"],
            "availability_topic": avail, "device": dev,
        }), retain=True)
        # Timer start/stop. The app STARTS the countdown with {"res1": 0}
        # (DeviceContentActivity "start_temp" button) and has no explicit
        # stop — stopping is timer 0h+0m (= timer off per cloud def) or heat off.
        # startTimer is true only when on && res1==0 && res2==0 (DeviceStatus.java).
        client.publish(f"{DISCOVERY_PREFIX}/button/{node}_timer_start/config", json.dumps({
            "name": "Vesta Timer Start", "unique_id": f"{node}_timer_start",
            "command_topic": set_topic, "payload_press": '{"res1": 0}',
            "availability_topic": avail, "device": dev,
        }), retain=True)
        client.publish(f"{DISCOVERY_PREFIX}/button/{node}_timer_stop/config", json.dumps({
            "name": "Vesta Timer Stop", "unique_id": f"{node}_timer_stop",
            "command_topic": set_topic, "payload_press": '{"set_time_hour": 0, "set_time_min": 0}',
            "availability_topic": avail, "device": dev,
        }), retain=True)
        # Alert bits (flat 0/1 ints in raw; derived "alerts" holds only active ones)
        alert_class = {"loss_power": "problem", "low_water_level": "problem",
                       "no_water": "problem", "not_working_properly": "problem"}
        alert_names = {"cooking_finish": "Vesta Cooking Finished", "loss_power": "Vesta Power Loss",
                       "low_water_level": "Vesta Low Water", "no_water": "Vesta No Water",
                       "water_hated": "Vesta At Temperature", "work_alert": "Vesta Work Reminder",
                       "not_working_properly": "Vesta Malfunction", "word_hour100": "Vesta 100h Service"}
        for key, label in alert_names.items():
            cfg = {"name": label, "unique_id": f"{node}_{key}",
                   "state_topic": state_topic,
                   "value_template": f"{{% if '{key}' in value_json.alerts %}}ON{{% else %}}OFF{{% endif %}}",
                   "availability_topic": avail, "device": dev}
            if key in alert_class:
                cfg["device_class"] = alert_class[key]
            client.publish(f"{DISCOVERY_PREFIX}/binary_sensor/{node}_{key}/config", json.dumps(cfg), retain=True)
        # NOTE: error_code bit decoding (decode_faults) stays observational only:
        # bit 0 survived a real E01 episode, a clear display, AND a 10s power
        # cycle, so it is permanent status, not a fault. fault_code/fault_bits
        # remain in the state topic for correlation. If the display ever shows
        # E0x again, compare against state.fault_bits live.

    def on_ha_birth(client, userdata, msg):
        # HA (re)started -> resend discovery so it re-discovers us.
        if msg.payload.decode() == "online":
            publish_discovery(client)

    mc.message_callback_add(f"{DISCOVERY_PREFIX}/status", on_ha_birth)

    def on_connect(client, userdata, flags, reason_code, properties=None):
        # (Re)subscribe + mark online + (re)publish discovery on every
        # (re)connect, so a dropped connection never leaves a stale
        # retained "offline" behind and HA always finds us.
        try:
            subs = [(set_topic, 0), (f"{DISCOVERY_PREFIX}/status", 0)]
            if recipes:
                subs.append((f"{recipe_base}/+/set", 0))
            client.subscribe(subs)
            client.publish(state_topic + "/availability", "online", retain=True)
            publish_discovery(client)
        except Exception:
            traceback.print_exc()

    mc.on_connect = on_connect
    mc.connect_async(MQTT_HOST, MQTT_PORT, 60)
    mc.loop_start()

    if ws_host and "--no-ws" not in sys.argv:
        threading.Thread(target=ws_loop,
                         args=(uid, token, did, ws_host, ws_port, on_ws_attrs),
                         daemon=True).start()
    else:
        print("ws: disabled (no host from bindings, or --no-ws)")

    once = "--once" in sys.argv
    while True:
        got = False
        if use_local:
            try:
                if not lan_ensure():
                    raise RuntimeError("lan unavailable (no session)")
                with lan_lock:
                    attrs = lan["session"].read_status()
                with raw_lock:
                    last_raw.clear()
                    last_raw.update(attrs)
                    publish_state("lan")
                got = True
            except Exception:
                traceback.print_exc()
                lan_close()
        if not got:
            latest = api("GET", f"/app/devdata/{did}/latest", token)
            with raw_lock:
                last_raw.clear()
                last_raw.update(derive(latest)["raw"])
                publish_state("polled")
        if once:
            break
        time.sleep(POLL)


if __name__ == "__main__":
    main()
