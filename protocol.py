# =============================================================================
# protocol.py
# -----------------------------------------------------------------------------
# Diese Datei ist die "gemeinsame Sprache" von Server und Client.
#
# Damit zwei Programme über das Netzwerk miteinander reden können, müssen sie
# sich auf ein Format einigen: Wie sieht eine Nachricht aus? Welche Arten von
# Nachrichten gibt es? Genau das steht hier. Sowohl der Server als auch der
# Client importieren diese Datei, damit beide dieselben Regeln benutzen.
# =============================================================================

import json      # Wandelt Python-Objekte (Dictionaries) in Text um und zurück.
import time      # Für Zeitstempel (aktuelle Uhrzeit).
import uuid      # Erzeugt zufällige, eindeutige Kennungen (IDs).
from typing import Any, Dict

# Texte werden als "UTF-8" kodiert - ein Standard, der alle Sonderzeichen kann.
ENCODING = "utf-8"

# Maximale Grösse (in Bytes) eines einzelnen UDP-Pakets, das wir empfangen.
BUFFER_SIZE = 65535


# -----------------------------------------------------------------------------
# Nachrichten-Typen
# -----------------------------------------------------------------------------
# Jede Nachricht hat einen "type". Damit wir uns nicht vertippen, bekommt jeder
# Typ hier einen festen Namen (eine Konstante). So steht z.B. immer exakt
# "JOIN_REQUEST" im Code statt mal "join" und mal "Join".

# --- Nachrichten zwischen Client und Server ---
DISCOVERY_REQUEST = "DISCOVERY_REQUEST"      # Client fragt per Broadcast: "Wo ist der Coordinator?"
DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"    # Server antwortet: "Hier ist der Coordinator."
JOIN_REQUEST = "JOIN_REQUEST"                # Client möchte einem Chatraum beitreten.
JOIN_ACCEPTED = "JOIN_ACCEPTED"              # Server bestätigt den Beitritt.
CHAT_MESSAGE = "CHAT_MESSAGE"                # Eine vom Client getippte Chat-Nachricht.
ORDERED_MESSAGE = "ORDERED_MESSAGE"          # Eine vom Coordinator nummerierte/sortierte Nachricht.
LEAVE = "LEAVE"                              # Client verlässt den Chat.
REDIRECT = "REDIRECT"                        # "Ich bin nicht der Coordinator, frag woanders."
ERROR = "ERROR"                              # Etwas ist schiefgelaufen.

# --- Nachrichten zwischen den Servern untereinander ---
SERVER_ANNOUNCE = "SERVER_ANNOUNCE"          # "Hallo, ich bin ein neuer Server."
STATE_SYNC = "STATE_SYNC"                    # Coordinator schickt Backups eine Kopie aller Daten.
HEARTBEAT = "HEARTBEAT"                      # "Ich (der Coordinator) lebe noch!" (regelmässig)
ELECTION = "ELECTION"                        # Teil der Wahl: "Ich möchte Coordinator werden."
ELECTION_OK = "ELECTION_OK"                  # Wahl-Antwort: "Du nicht, ich habe eine höhere ID."
COORDINATOR_ANNOUNCE = "COORDINATOR_ANNOUNCE"  # "Ich bin ab jetzt der neue Coordinator."


def now_ms() -> int:
    """Gibt die aktuelle Uhrzeit in Millisekunden zurück (als ganze Zahl).
    Dient als Zeitstempel, damit man weiss, wann eine Nachricht entstand."""
    return int(time.time() * 1000)


def new_id(prefix: str) -> str:
    """Erzeugt eine eindeutige Kennung, z.B. 'client-1a2b3c4d'.
    'uuid4().hex' ist eine zufällige Zeichenkette; wir nehmen die ersten 8
    Zeichen, damit es kurz und trotzdem praktisch eindeutig bleibt."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def make_message(message_type: str, **payload: Any) -> Dict[str, Any]:
    """Baut eine Nachricht zusammen. Eine Nachricht ist immer ein Dictionary mit
    drei Teilen:
      - 'type':         um welche Art Nachricht es sich handelt
      - 'timestamp_ms': wann sie erstellt wurde
      - 'payload':      die eigentlichen Daten (Inhalt)

    Das '**payload' bedeutet: Alle zusätzlich übergebenen benannten Argumente
    landen gesammelt im payload. Beispiel:
        make_message(CHAT_MESSAGE, text="Hallo")
        -> {'type': 'CHAT_MESSAGE', 'timestamp_ms': ..., 'payload': {'text': 'Hallo'}}
    """
    return {
        "type": message_type,
        "timestamp_ms": now_ms(),
        "payload": payload,
    }


def encode(message: Dict[str, Any]) -> bytes:
    """Wandelt eine Nachricht (Dictionary) in Bytes um, damit man sie über das
    Netzwerk verschicken kann. Netzwerke transportieren nur Bytes, kein Python.
    'json.dumps' macht aus dem Dictionary einen Text, '.encode' macht Bytes draus.
    'separators=(",", ":")' entfernt unnötige Leerzeichen -> kompakter."""
    return json.dumps(message, separators=(",", ":")).encode(ENCODING)


def decode(data: bytes) -> Dict[str, Any]:
    """Das Gegenstück zu encode(): wandelt empfangene Bytes wieder in ein
    Python-Dictionary zurück, mit dem man arbeiten kann."""
    return json.loads(data.decode(ENCODING))


def send_json_tcp(sock, message: Dict[str, Any]) -> None:
    """Sendet eine Nachricht über eine TCP-Verbindung.

    Wichtig: Bei TCP kommen Daten als kontinuierlicher "Strom" an, ohne klare
    Grenzen zwischen einzelnen Nachrichten. Damit der Empfänger weiss, wo eine
    Nachricht endet, hängen wir ein Zeilenende '\\n' an. Der Empfänger (siehe
    read_json_lines) trennt den Strom dann genau an diesen Zeilenenden."""
    raw = encode(message) + b"\n"
    sock.sendall(raw)


def read_json_lines(sock):
    """Liest fortlaufend Nachrichten aus einer TCP-Verbindung.

    Das ist ein 'Generator': Er liefert eine Nachricht nach der anderen zurück
    (mit 'yield'), sobald sie vollständig empfangen wurde. Man kann ihn in einer
    for-Schleife benutzen: 'for nachricht in read_json_lines(sock): ...'.

    Ablauf:
      1. Wir sammeln ankommende Bytes in 'buffer'.
      2. Liefert recv() nichts (leer), wurde die Verbindung geschlossen -> Ende.
      3. Sobald im Puffer ein '\\n' steht, ist (mindestens) eine Nachricht
         komplett. Wir schneiden sie ab, wandeln sie zurück und geben sie aus.
    """
    buffer = b""
    while True:
        chunk = sock.recv(4096)          # Bis zu 4096 Bytes aus dem Netzwerk holen.
        if not chunk:                    # Leer = Gegenseite hat die Verbindung beendet.
            return
        buffer += chunk
        # Es können mehrere Nachrichten auf einmal angekommen sein -> alle abarbeiten.
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)   # Beim ersten '\n' aufteilen.
            if line.strip():                         # Leere Zeilen ignorieren.
                yield decode(line)                   # Fertige Nachricht zurückgeben.
