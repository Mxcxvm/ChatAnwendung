import json
import time
import uuid
from typing import Any, Dict

ENCODING = "utf-8"
BUFFER_SIZE = 65535

DISCOVERY_REQUEST = "DISCOVERY_REQUEST"
DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"

JOIN_REQUEST = "JOIN_REQUEST"
JOIN_ACCEPTED = "JOIN_ACCEPTED"
JOIN_REJECTED = "JOIN_REJECTED"

CHAT_MESSAGE = "CHAT_MESSAGE"
ORDERED_MESSAGE = "ORDERED_MESSAGE"
LEAVE = "LEAVE"

SERVER_ANNOUNCE = "SERVER_ANNOUNCE"
STATE_SYNC = "STATE_SYNC"
HEARTBEAT = "HEARTBEAT"

ELECTION = "ELECTION"
ELECTION_OK = "ELECTION_OK"
COORDINATOR_ANNOUNCE = "COORDINATOR_ANNOUNCE"

REDIRECT = "REDIRECT"
ERROR = "ERROR"


def now_ms() -> int:
    return int(time.time() * 1000)


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def make_message(message_type: str, **payload: Any) -> Dict[str, Any]:
    return {
        "type": message_type,
        "timestamp_ms": now_ms(),
        "payload": payload,
    }


def encode(message: Dict[str, Any]) -> bytes:
    return json.dumps(message, separators=(",", ":")).encode(ENCODING)


def decode(data: bytes) -> Dict[str, Any]:
    return json.loads(data.decode(ENCODING))


def send_json_tcp(sock, message: Dict[str, Any]) -> None:
    raw = encode(message) + b"\n"
    sock.sendall(raw)


def read_json_lines(sock):
    buffer = b""

    while True:
        chunk = sock.recv(4096)

        if not chunk:
            return

        buffer += chunk

        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)

            if line.strip():
                yield decode(line)