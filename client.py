# =============================================================================
# client.py
# -----------------------------------------------------------------------------
# Das Chat-Programm fuer den Benutzer.
#
# Aufgabe des Clients:
#   1. Den aktuellen "Coordinator"-Server im lokalen Netzwerk finden (per Broadcast).
#   2. Sich mit ihm verbinden und einem Chatraum beitreten.
#   3. Getippte Nachrichten senden und empfangene Nachrichten anzeigen.
#   4. Stirbt der Coordinator, automatisch den neuen Coordinator finden und sich
#      neu verbinden - ohne dass eine Nachricht verloren geht.
#
# Hintergrund: Es laufen mehrere Server. Genau einer davon ist der "Coordinator"
# (der Chef), die anderen sind "Backups" (Reserve). Nur der Coordinator bedient
# Clients. Faellt er aus, waehlen die Backups untereinander einen neuen.
# =============================================================================

import argparse                       # Liest Kommandozeilen-Argumente (z.B. --username).
import socket                         # Netzwerk-Kommunikation (Verbindungen, Datenpakete).
import threading                      # Mehrere Dinge gleichzeitig tun (z.B. senden + empfangen).
import time                           # Warten/Pausen.
from collections import OrderedDict   # Ein Dictionary, das sich die Reihenfolge merkt.
from typing import Optional, Dict

from protocol import *                # Die gemeinsame "Sprache" (Nachrichten-Typen, Hilfsfunktionen).

# Standardadresse fuer einen "Broadcast": eine Nachricht an ALLE Geraete im
# lokalen Netzwerk gleichzeitig. So findet der Client den Server, ohne dessen
# IP-Adresse vorher zu kennen.
BROADCAST_ADDRESS = "255.255.255.255"

# Fester Port (eine Art "Tuernummer"), auf dem die Server auf Suchanfragen hoeren.
DISCOVERY_PORT = 5973

RECONNECT_DELAY = 2.0
DISCOVERY_TIMEOUT = 2.0
CONNECT_TIMEOUT = 5.0

RECV_TIMEOUT = 0.5
SERVER_SILENCE_TIMEOUT = 5.0


def valid_coordinator(coordinator: Optional[Dict[str, Any]]) -> bool:
    if not coordinator:
        return False

    try:
        host = coordinator["host"]
        port = int(coordinator["client_port"])

        if not host or port <= 0:
            return False

        socket.gethostbyname(host)
        return True

    except Exception:
        return False


def local_ip() -> Optional[str]:
    """Findet die eigene IP-Adresse im lokalen Netzwerk heraus.

    Trick: Wir 'verbinden' ein UDP-Socket zu einer beliebigen externen Adresse
    (8.8.8.8, ein Google-Server). Dabei wird tatsaechlich NICHTS gesendet - das
    Betriebssystem waehlt nur die Netzwerkkarte aus, ueber die es gehen wuerde,
    und verraet uns so unsere eigene Adresse in diesem Netz."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]   # Unsere eigene IP, die das OS gewaehlt hat.
    except OSError:
        return None                    # Keine Netzwerkverbindung -> nichts gefunden.
    finally:
        sock.close()


def broadcast_targets(broadcast_address: str) -> list:
    """Liefert die Zieladressen, an die wir die Suchanfrage schicken.

    Wir verschicken an ZWEI Adressen, um zuverlaessiger zu sein:
      1. den allgemeinen Broadcast (255.255.255.255)
      2. den 'gerichteten' Broadcast des eigenen Netzes, z.B. 192.168.178.255
         (das '.255' am Ende heisst 'alle im Netz 192.168.178.x').
    Manche Netzwerke/Router behandeln die eine oder andere Variante besser."""
    targets = [broadcast_address]
    ip = local_ip()
    if ip:
        # Aus z.B. "192.168.178.46" wird "192.168.178" + ".255" = "192.168.178.255".
        directed = ip.rsplit(".", 1)[0] + ".255"
        if directed not in targets:
            targets.append(directed)
    return targets


def discover_coordinator(
    broadcast_address: str,
    discovery_port: int,
    timeout: float = 2.0
) -> Optional[Dict]:
    """Sucht den aktuellen Coordinator im Netzwerk und gibt dessen Infos zurueck
    (oder None, wenn keiner antwortet).

    Ablauf:
      1. Eine 'Suchanfrage' (DISCOVERY_REQUEST) per Broadcast an alle senden.
      2. Auf Antworten warten (maximal 'timeout' Sekunden).
      3. Die erste Antwort, die einen Coordinator enthaelt, zurueckgeben.
    """
    # UDP-Socket: fuer kurze, einzelne Datenpakete (verbindungslos) - ideal fuer Suche.
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Diese Option MUSS gesetzt sein, damit man ueberhaupt Broadcasts senden darf.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)           # Nicht ewig warten, sondern hoechstens 'timeout'.

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)

        request = make_message(DISCOVERY_REQUEST, client_probe=True)
        # An alle Ziel-Broadcastadressen senden.
        for target in broadcast_targets(broadcast_address):
            try:
                sock.sendto(encode(request), (target, discovery_port))
            except OSError:
                pass                   # Klappt eine Adresse nicht, einfach weiter.

        deadline = time.time() + timeout   # Zeitpunkt, ab dem wir aufhoeren zu warten.
        best = None

        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(BUFFER_SIZE)   # Auf eine Antwort warten.
                response = decode(data)

                # Uns interessieren nur DISCOVERY_RESPONSE-Nachrichten.
                if response.get("type") != DISCOVERY_RESPONSE:
                    continue

                payload = response.get("payload", {})
                coordinator = payload.get("coordinator")

                if coordinator:        # Antwort enthaelt einen Coordinator -> fertig.
                    best = coordinator
                    break

            except socket.timeout:
                break                  # Niemand hat (mehr) geantwortet.

        if not candidates:
            return None

        def sort_key(coord: Dict[str, Any]) -> int:
            try:
                return int(coord.get("server_id", 0))
            except Exception:
                return 0

        candidates.sort(key=sort_key, reverse=True)
        return candidates[0]

    finally:
        sock.close()                   # Socket immer schliessen, egal was passiert.


class ChatClient:
    """Chat-Client mit automatischem Reconnect.

    Die Klasse buendelt den gesamten Zustand des Clients (Verbindung, offene
    Nachrichten usw.) und die noetigen Funktionen. Kernidee fuer die
    Ausfallsicherheit:
      - Ein Hintergrund-Thread (connection_manager) kuemmert sich darum, IMMER
        mit dem aktuellen Coordinator verbunden zu sein. Bricht die Verbindung,
        sucht er automatisch den neuen Coordinator und verbindet neu.
      - Eine 'Outbox' merkt sich gesendete, aber noch nicht bestaetigte
        Nachrichten, damit beim Coordinator-Wechsel nichts verloren geht.
    """

    # Wie lange wir nach einem Fehlversuch warten, bevor wir es erneut probieren.
    RECONNECT_DELAY = 1.0

    def __init__(self, username: str, room: str, client_id: Optional[str],
                 broadcast_address: str, discovery_port: int):
        self.username = username                       # Anzeigename im Chat.
        self.room = room                               # Name des Chatraums.
        # Eindeutige Kennung dieses Clients. Bleibt ueber Reconnects gleich,
        # damit der Server uns wiedererkennt. Wird automatisch erzeugt, falls keine angegeben.
        self.client_id = client_id or new_id("client")
        self.broadcast_address = broadcast_address
        self.discovery_port = discovery_port

        self.sock: Optional[socket.socket] = None      # Aktuelle Verbindung (oder None).
        # Ein 'Lock' verhindert, dass zwei Threads gleichzeitig dieselben Daten
        # veraendern und sich dabei in die Quere kommen (Datensalat).
        self.lock = threading.Lock()
        # Ein 'Event' ist ein Schalter: gesetzt = "wir sind verbunden".
        self.connected = threading.Event()
        self.running = True                            # Laeuft das Programm noch?

        # Outbox: noch nicht vom Coordinator bestaetigte Nachrichten.
        # Sie bleiben hier liegen, bis ihr "Echo" (ORDERED_MESSAGE) zurueckkommt,
        # und werden nach jedem (Re-)Connect erneut gesendet -> kein Verlust beim Failover.
        self.counter = 0                               # Zaehlt unsere Nachrichten durch.
        self.outbox: "OrderedDict[str, Dict]" = OrderedDict()

    def run(self) -> None:
        """Startet den Client und liest dann Tastatureingaben des Benutzers."""
        # Verbindungspflege laeuft im Hintergrund (daemon=True: stirbt mit dem Programm).
        threading.Thread(target=self.connection_manager, daemon=True).start()
        try:
            while self.running:
                # input() wartet, bis der Benutzer eine Zeile tippt und Enter drueckt.
                try:
                    text = input()
                except EOFError:                     
                    break

                # Sonderbefehle zum Beenden.
                if text.strip().lower() in {"/quit", "/exit"}:
                    self.send(make_message(LEAVE))     
                    break

                self.send_chat(text)                   # Normale Chat-Nachricht verschicken.
        finally:
            # Sauber aufraeumen, wenn die Schleife endet.
            self.running = False
            self.connected.clear()
            with self.lock:
                if self.sock:
                    try:
                        self.sock.close()
                    except OSError:
                        pass

    def send(self, message: Dict) -> None:
        """Sendet eine fertige Nachricht ueber die aktuelle Verbindung.
        Wird z.B. fuer die LEAVE-Nachricht benutzt. Bei Fehlern geben wir nur
        einen Hinweis aus - der connection_manager baut die Verbindung ggf. neu auf."""
        with self.lock:
            sock = self.sock
        if not sock:
            return
        try:
            send_json_tcp(sock, message)
        except OSError:
            print("(Senden fehlgeschlagen - Verbindung wird neu aufgebaut)")

    def send_chat(self, text: str) -> None:
        """Verschickt eine Chat-Nachricht - mit Verlustschutz.

        Wichtig fuer die Ausfallsicherheit: Die Nachricht bekommt eine eigene ID
        und wandert in die Outbox, BEVOR sie gesendet wird. Schlaegt das Senden
        fehl oder besteht gerade keine Verbindung, bleibt sie dort liegen und
        wird nach dem naechsten Reconnect erneut gesendet. Endgueltig 'erledigt'
        ist sie erst, wenn ihr Echo vom Coordinator zurueckkommt."""
        with self.lock:
            self.counter += 1
            # Eindeutige Nachrichten-ID, z.B. "client-1a2b3c4d:5".
            msg_id = f"{self.client_id}:{self.counter}"
            message = make_message(CHAT_MESSAGE, text=text, msg_id=msg_id)
            self.outbox[msg_id] = message              # In die Outbox legen.
            # Nur senden, wenn wir gerade wirklich verbunden sind.
            sock = self.sock if self.connected.is_set() else None
        if sock is None:
            print("(offline - Nachricht wird nach Reconnect gesendet)")
            return
        try:
            send_json_tcp(sock, message)
        except OSError:
            print("(Senden fehlgeschlagen - Nachricht wird nach Reconnect gesendet)")

    def resend_outbox(self, sock: socket.socket) -> None:
        """Sendet alle noch unbestaetigten Nachrichten aus der Outbox erneut.
        Wird direkt nach jedem (Re-)Connect aufgerufen, damit Nachrichten, die
        waehrend eines Ausfalls 'haengen geblieben' sind, doch noch ankommen."""
        with self.lock:
            pending = list(self.outbox.values())       # Kopie der offenen Nachrichten.
        for message in pending:
            try:
                send_json_tcp(sock, message)
            except OSError:
                return                                 # Verbindung wieder weg -> abbrechen.

    def connection_manager(self) -> None:
        """Der 'Verbindungs-Manager' - laeuft dauerhaft im Hintergrund.

        Endlosschleife: Coordinator suchen -> verbinden -> beitreten -> offene
        Nachrichten nachsenden -> empfangen, bis die Verbindung abbricht ->
        wieder von vorne. So ist der Client immer mit dem aktuellen Coordinator
        verbunden, auch nach einem Serverausfall."""
        while self.running:
            # 1. Aktuellen Coordinator im Netzwerk suchen.
            coordinator = discover_coordinator(self.broadcast_address, self.discovery_port)

            if not coordinator:
                time.sleep(self.RECONNECT_DELAY)       # Keiner da -> kurz warten, erneut suchen.
                continue

            # 2. Verbinden und dem Chatraum beitreten.
            sock = self.connect_and_join(coordinator)
            if sock is None:                           # Verbindung fehlgeschlagen.
                time.sleep(self.RECONNECT_DELAY)       # (z.B. weil die Wahl noch laeuft)
                continue

            # 3. Verbindung als 'aktiv' markieren.
            with self.lock:
                self.sock = sock
            self.connected.set()
            print(
                f"Verbunden mit Coordinator {coordinator['server_id']} "
                f"@ {coordinator['host']}:{coordinator['client_port']}"
            )

            # 4. Alle noch unbestaetigten Nachrichten erneut senden.
            self.resend_outbox(sock)

            # 5. Nachrichten empfangen, bis die Verbindung abbricht (blockiert hier).
            self.receive_until_disconnect(sock)

            # 6. Verbindung ist weg -> Status zuruecksetzen.
            self.connected.clear()
            with self.lock:
                self.sock = None

            if self.running:
                print("Verbindung zum Coordinator verloren. Suche neuen Coordinator...")
                time.sleep(self.RECONNECT_DELAY)       # Kurz warten (die Wahl braucht etwas).

    def connect_and_join(self, coordinator: Dict) -> Optional[socket.socket]:
        """Baut eine TCP-Verbindung zum Coordinator auf und meldet sich an.
        Gibt das verbundene Socket zurueck - oder None, wenn es nicht klappt."""
        # TCP-Socket: fuer eine stabile, dauerhafte Verbindung (anders als UDP).
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(5.0)                       # Hoechstens 5s auf den Verbindungsaufbau warten.
            sock.connect((coordinator["host"], int(coordinator["client_port"])))
            sock.settimeout(None)                      # Danach wieder unbegrenzt (zum Empfangen).
            # Beitrittsanfrage an den Chatraum senden.
            send_json_tcp(sock, make_message(
                JOIN_REQUEST,
                client_id=self.client_id,
                username=self.username,
                room=self.room,
            ))
            return sock
        except OSError:
            sock.close()                               # Bei Fehler Socket schliessen.
            return None

    def receive_until_disconnect(self, sock: socket.socket) -> None:
        """Empfaengt und verarbeitet Nachrichten vom Server, bis die Verbindung
        endet. Laeuft so lange, wie Daten kommen; bricht die Verbindung ab, kehrt
        die Funktion zurueck (und der connection_manager verbindet neu)."""
        try:
            for message in read_json_lines(sock):
                msg_type = message.get("type")
                payload = message.get("payload", {})

                if msg_type == JOIN_ACCEPTED:
                    # Beitritt bestaetigt: Raum, Teilnehmer und letzte Nachrichten anzeigen.
                    print(f"Joined room '{payload['room']}' as {payload['client_id']}")
                    print("Participants:", ", ".join(payload.get("participants", [])))
                    for item in payload.get("recent_messages", []):
                        print(f"#{item['sequence']} {item['sender_name']}: {item['text']}")

                elif msg_type == ORDERED_MESSAGE:
                    # Eine vom Coordinator nummerierte Chat-Nachricht.
                    item = payload["message"]
                    # Ist es das Echo einer EIGENEN Nachricht? Dann gilt sie als
                    # bestaetigt und kann aus der Outbox entfernt werden.
                    mid = item.get("msg_id")
                    if mid:
                        with self.lock:
                            self.outbox.pop(mid, None)
                    # Nachricht anzeigen ('#sequence' = die globale Reihenfolge-Nummer).
                    print(f"#{item['sequence']} {item['sender_name']}: {item['text']}")

                elif msg_type == REDIRECT:
                    # Der Server ist nicht (mehr) der Coordinator -> Schleife verlassen,
                    # damit der connection_manager den richtigen Coordinator sucht.
                    break

                elif msg_type == ERROR:
                    print("Error:", payload.get("reason"))

        except OSError:
            pass                                       # Verbindung weg -> einfach beenden.
        finally:
            try:
                sock.close()
            except OSError:
                pass


def main() -> None:
    """Einstiegspunkt: liest die Kommandozeilen-Argumente und startet den Client."""
    parser = argparse.ArgumentParser(
        description="Chat client using broadcast discovery"
    )

    # --username ist Pflicht, der Rest hat sinnvolle Standardwerte.
    parser.add_argument("--username", required=True)
    parser.add_argument("--room", default="general")
    parser.add_argument("--client-id", default=None)

    parser.add_argument("--broadcast-address", default=BROADCAST_ADDRESS)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)

    args = parser.parse_args()

    # Client-Objekt mit den gewaehlten Einstellungen erstellen und starten.
    ChatClient(
        username=args.username,
        room=args.room,
        client_id=args.client_id,
        broadcast_address=args.broadcast_address,
        discovery_port=args.discovery_port,
    ).run()


# Diese Zeile sorgt dafuer, dass main() nur laeuft, wenn man die Datei direkt
# startet (python client.py) - und nicht, wenn sie nur importiert wird.
if __name__ == "__main__":
    main()
