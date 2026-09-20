"""Gizwits GAgent LAN session for the Vesta sous vide cooker.

Same wire protocol the vendor app uses on the local network (see
jebao/node-ph803w precedent + GAgent sources; probes in tooling/lan_*.py):
- UDP :12414 discovery (broadcast 00000003 03000003)
- TCP :12416 session: passcode login (0x08/0x09), status read (0x90/0x91),
  control write (0x93/0x94 with u32 seq), heartbeat (0x15) every few seconds.

Status blobs decode with the datapoint byte positions from the cloud
/app/datapoint definition, so attr names match the OpenAPI shapes exactly
(data dict + flat alert bits). stdlib only.
"""
import socket
import struct
import threading
import time

UDP_PORT = 12414
TCP_PORT = 12416
DISCOVER_FRAME = bytes.fromhex("0000000303000003")

CMD_PASSCODE_REQUEST = 0x0006
CMD_LOGIN_REQUEST = 0x0008
CMD_LOGIN_RESPONSE = 0x0009
CMD_HEARTBEAT = 0x0015
CMD_TRANSMIT_REQUEST = 0x0090
CMD_TRANSMIT_RESPONSE = 0x0091
CMD_CONTROL_REQUEST = 0x0093
CMD_CONTROL_RESPONSE = 0x0094
P0_READ_STATUS = 0x02
P0_STATUS_REPLY = 0x03
P0_STATUS_REPORT = 0x04
P0_CONTROL = 0x01

# attr ids (cloud /app/datapoint definition)
ID = {"onoff": 0, "clean_model": 1, "dev_reset": 2, "set_temp_integer": 3,
      "set_temp_decimal": 4, "temp_unit": 5, "set_time_hour": 6,
      "set_time_min": 7, "res1": 8, "res2": 9}
BYTE = {"set_temp_integer": 1, "set_temp_decimal": 2, "temp_unit": 3,
        "set_time_hour": 4, "set_time_min": 5, "res1": 6, "res2": 7}
# (name, kind, byte, bit)
FIELDS = [
    ("onoff", "bit", 0, 0), ("clean_model", "bit", 0, 1), ("dev_reset", "bit", 0, 2),
    ("set_temp_integer", "u8", 1, 0), ("set_temp_decimal", "u8", 2, 0),
    ("temp_unit", "u8", 3, 0), ("set_time_hour", "u8", 4, 0),
    ("set_time_min", "u8", 5, 0), ("res1", "u8", 6, 0), ("res2", "u8", 7, 0),
    ("error_code", "u16", 8, 0),
    ("real_temp_integer", "u8", 10, 0), ("real_temp_decimal", "u8", 11, 0),
    ("runnning_time_hour", "u8", 12, 0), ("runnning_time_min", "u8", 13, 0),
    ("remaining_time_hour", "u8", 14, 0), ("remaining_time_min", "u8", 15, 0),
    ("work_alert", "bit", 16, 0), ("water_hated", "bit", 16, 1),
    ("word_hour100", "bit", 16, 2), ("no_water", "bit", 16, 3),
    ("low_water_level", "bit", 16, 4), ("cooking_finish", "bit", 16, 5),
    ("loss_power", "bit", 16, 6), ("not_working_properly", "bit", 16, 7),
]


class LANError(RuntimeError):
    pass


def encode_uvarint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def encode_frame(cmd, payload=b""):
    body = b"\x00" + struct.pack(">H", cmd) + payload
    return b"\x00\x00\x00\x03" + encode_uvarint(len(body)) + body


def decode_blob(blob):
    """17-byte P0 status -> cloud-style attr dict (alert bits flat, 0/1)."""
    if len(blob) < 17:
        raise LANError(f"short status blob: {len(blob)}")
    vals = {}
    for name, kind, byte, bit in FIELDS:
        if kind == "bit":
            vals[name] = int(bool(blob[byte] >> bit & 1))
        elif kind == "u8":
            vals[name] = blob[byte]
        else:
            vals[name] = struct.unpack(">H", blob[byte:byte + 2])[0]
    vals["error_code"] = f"{blob[8]:02x}{blob[9]:02x}"
    return vals


def build_control(changes):
    """attr dict -> p0 control payload (action + reversed flags + vals)."""
    flags = bytearray(2)
    vals = bytearray(8)
    for name, v in changes.items():
        i = ID[name]
        flags[i // 8] |= 1 << (i % 8)
        if name == "onoff":
            if v:
                vals[0] |= 1
        else:
            vals[BYTE[name]] = int(v) & 0xFF
    return b"\x01" + bytes(reversed(flags)) + bytes(vals)


def discover(timeout=8, did=None):
    """Broadcast discovery, return [{ip, did, mac, product_key}, ...]."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.settimeout(timeout)
    s.bind(("0.0.0.0", 0))
    s.sendto(DISCOVER_FRAME, ("255.255.255.255", UDP_PORT))
    found, end = {}, time.time() + timeout
    while time.time() < end:
        try:
            data, addr = s.recvfrom(4096)
        except socket.timeout:
            break
        try:
            if data[:4] != b"\x00\x00\x00\x03" or data[4] & 0x80:
                continue
            body = data[5:]
            if body[0] != 0x00 or body[1:3] != b"\x00\x04":
                continue
            off, parts = 3, []
            for _ in range(4):
                ln = struct.unpack_from(">H", body, off)[0]
                off += 2
                parts.append(body[off:off + ln])
                off += ln
            dev = {"ip": addr[0],
                   "did": parts[0].decode("ascii", errors="replace"),
                   "mac": parts[1].hex(),
                   "product_key": parts[3].decode("ascii", errors="replace")}
            if (did is None or dev["did"] == did) and addr[0] not in found:
                found[addr[0]] = dev
        except (IndexError, struct.error):
            continue
    s.close()
    return list(found.values())


class GAgentSession:
    """Blocking TCP session with background heartbeat. Not thread-safe for
    concurrent calls — the bridge serializes via its own lock."""

    def __init__(self, ip, passcode, port=TCP_PORT):
        self.ip, self.port = ip, port
        self.passcode = passcode.encode() if isinstance(passcode, str) else passcode
        self.sock = None
        self.seq = 0
        self._hb_stop = threading.Event()
        self._hb_thread = None

    def connect(self):
        self.close()
        self.sock = socket.create_connection((self.ip, self.port), timeout=10)
        self.sock.settimeout(12)
        self._send(CMD_LOGIN_REQUEST,
                   struct.pack(">H", len(self.passcode)) + self.passcode)
        cmd, payload = self._recv()
        if cmd != CMD_LOGIN_RESPONSE or not payload or payload[-1] != 0x00:
            raise LANError(f"login rejected: cmd={cmd:#06x}")
        self._hb_stop.clear()
        self._hb_thread = threading.Thread(target=self._heartbeat, daemon=True)
        self._hb_thread.start()

    def close(self):
        self._hb_stop.set()
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def _heartbeat(self):
        while not self._hb_stop.wait(4):
            try:
                self._send(CMD_HEARTBEAT)
            except OSError:
                return

    def _send(self, cmd, payload=b""):
        self.sock.sendall(encode_frame(cmd, payload))

    def _recv(self):
        hdr = b""
        while len(hdr) < 4:
            chunk = self.sock.recv(4 - len(hdr))
            if not chunk:
                raise LANError("connection closed")
            hdr += chunk
        if hdr != b"\x00\x00\x00\x03":
            raise LANError(f"bad magic: {hdr.hex()}")
        length, shift = 0, 0
        while True:
            b = self.sock.recv(1)[0]
            length |= (b & 0x7F) << shift
            if not b & 0x80:
                break
            shift += 7
        body = b""
        while len(body) < length:
            body += self.sock.recv(length - len(body))
        return struct.unpack(">H", body[1:3])[0], body[3:]

    def _drain(self):
        """Discard stale inbound frames (heartbeat acks, duplicate acks)
        so the next request's reply is found within a few skips."""
        old = self.sock.gettimeout()
        self.sock.settimeout(0.3)
        try:
            while True:
                self._recv()
        except (socket.timeout, LANError, OSError):
            pass
        finally:
            self.sock.settimeout(old or 12)

    def read_status(self):
        self._drain()
        self._send(CMD_TRANSMIT_REQUEST, bytes([P0_READ_STATUS]))
        for _ in range(10):
            cmd, payload = self._recv()
            if cmd == CMD_TRANSMIT_RESPONSE and payload \
                    and payload[0] in (P0_STATUS_REPLY, P0_STATUS_REPORT):
                return decode_blob(payload[1:])
        raise LANError("no status reply")

    def write(self, changes):
        self.seq += 1
        self._drain()
        self._send(CMD_CONTROL_REQUEST,
                   struct.pack(">I", self.seq) + build_control(changes))
        for _ in range(10):
            cmd, payload = self._recv()
            if cmd == CMD_CONTROL_RESPONSE:
                return payload
        raise LANError("no control ack")
