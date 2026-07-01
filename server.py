# =============================================================================
# server.py
# -----------------------------------------------------------------------------
# Der Chat-Server. Man startet mehrere davon.
#
# Grundidee:
#   - Es laufen mehrere gleichberechtigte Server.
#   - Genau EINER ist der "Coordinator" (der Chef): Nur er nimmt Clients an,
#     bringt Nachrichten in eine Reihenfolge und verteilt sie.
#   - Die anderen sind "Backups" (Reserve): Sie halten eine Kopie aller Daten
#     bereit, falls der Coordinator ausfällt.
#   - Fällt der Coordinator aus, wählen die Backups untereinander einen neuen.
#     Dafür wird der "Bully-Algorithmus" benutzt: Wer die HÖCHSTE ID hat, wird
#     Coordinator.
#
# Wichtige Bausteine in dieser Datei:
#   - Discovery: Clients finden den Coordinator per Broadcast.
#   - Heartbeat: Der Coordinator sendet regelmässig "ich lebe noch".
#   - Wahl (Election): Auswahl eines neuen Coordinators bei Ausfall.
#   - State-Sync: Der Coordinator spiegelt seinen Datenstand zu den Backups.
# =============================================================================

import argparse     
import random      
import socket     
import struct     
import threading  
import time         
from typing import Dict, Tuple, Optional

from protocol import *   

# Eine Multicast-Gruppe ist eine spezielle Adresse, über die sich die Server
# untereinander finden/ansprechen. 
MULTICAST_GROUP = "224.1.1.1"

# Fester Port, auf dem Suchanfragen und Server-Ankündigungen ankommen.
DISCOVERY_PORT = 5973

# Alle wie viele Sekunden der Coordinator ein "ich lebe noch" (Heartbeat) sendet.
HEARTBEAT_INTERVAL = 1.0


def detect_lan_ip() -> str:
    """Ermittelt die eigene LAN-IP (die Adresse, unter der ein Client uns
    erreichen kann). Trick wie beim Client: Wir 'verbinden' ein UDP-Socket zu
    einer externen Adresse - es wird nichts gesendet, das Betriebssystem wählt
    nur die passende Netzwerkkarte und verrät uns dabei unsere IP."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except OSError:
        return "127.0.0.1"     # Notfall: 'localhost' (nur dieses Gerät).
    finally:
        sock.close()


# Fällt vom Coordinator so lange kein Heartbeat, gilt er als tot (in Sekunden).
HEARTBEAT_TIMEOUT = 3.2

# Alle wie viele Sekunden der Coordinator seinen kompletten Datenstand spiegelt.
STATE_SYNC_INTERVAL = 2.0


class ChatServer:
    """Ein einzelner Server. Alle Daten und Verhaltensweisen stecken hier drin."""

    def __init__(self, server_id: int, host: str, client_port: int, server_port: int,
                 multicast_group: str = MULTICAST_GROUP, discovery_port: int = DISCOVERY_PORT):
        # --- Identität und Netzwerk-Einstellungen ---
        self.server_id = server_id           # Eindeutige ID. Höchste ID gewinnt die Wahl.
        self.host = host                     # Eigene Adresse, die wir Clients mitteilen.
        self.client_port = client_port       # TCP-Port für Clients (0 = OS wählt frei).
        self.server_port = server_port       # UDP-Port für Server-zu-Server (0 = OS wählt frei).
        self.multicast_group = multicast_group
        self.discovery_port = discovery_port

        # --- Rolle und bekannte Server ---
        self.role = "backup"                 # Jeder startet als "backup", bis die Wahl entscheidet.
        self.coordinator: Optional[Dict] = None   # Infos zum aktuellen Coordinator (oder None).
        # Alle bekannten Server, nach ihrer ID. Anfangs nur wir selbst.
        self.servers: Dict[int, Dict] = {
            self.server_id: self.server_info("backup")
        }

        # --- Chat-Daten ---
        self.clients: Dict[str, Dict] = {}   # Angemeldete Clients (client_id -> Infos).
        self.rooms: Dict[str, set] = {}      # Welche Clients sind in welchem Raum.
        self.message_history = []            # Alle bisherigen (geordneten) Nachrichten.
        self.global_sequence = 0             # Fortlaufende Nummer für die Reihenfolge.
        # Offene Netzwerkverbindungen zu den Clients (client_id -> Verbindung).
        self.client_connections: Dict[str, socket.socket] = {}
        # Dedup-Tabelle: vom Client vergebene msg_id -> bereits geordnete Nachricht.
        # Verhindert, dass eine erneut gesendete Nachricht doppelt erscheint.
        self.seen_msg_ids: Dict[str, Dict] = {}

        # --- Technische Helfer ---
        # RLock: Schloss gegen gleichzeitiges Ändern durch mehrere Threads.
        self.lock = threading.RLock()
        self.running = True                  # Läuft der Server noch?
        self.last_heartbeat = time.time()    # Wann kam zuletzt ein Lebenszeichen?
        self.election_in_progress = False    # Läuft gerade eine Wahl?

        # Diese Sockets werden in bind_sockets() erzeugt (Port 0 = vom OS frei gewählt).
        self.client_sock: Optional[socket.socket] = None
        self.server_sock: Optional[socket.socket] = None

    def bind_sockets(self) -> None:
        """Erstellt und 'bindet' die beiden Haupt-Sockets (reserviert die Ports).

        Wichtig: Wir machen das, BEVOR wir uns im Netzwerk ankündigen. Bei Port 0
        sucht das Betriebssystem selbst einen freien Port aus; wir lesen den
        tatsächlich vergebenen Port anschliessend aus, damit wir Clients und
        anderen Servern die richtige Portnummer nennen können."""
        # Client-Socket: TCP (stabile Verbindungen für Clients).
        cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cs.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        cs.bind(("0.0.0.0", self.client_port))   # "0.0.0.0" = auf allen Netzwerkkarten lauschen.
        cs.listen(50)                            # Bis zu 50 wartende Verbindungen zulassen.
        self.client_port = cs.getsockname()[1]   # Tatsächlich vergebenen Port merken.
        self.client_sock = cs

        # Server-Socket: UDP (kurze Nachrichten zwischen den Servern).
        ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ss.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        ss.bind(("0.0.0.0", self.server_port))
        self.server_port = ss.getsockname()[1]
        self.server_sock = ss

        # Unseren eigenen Eintrag mit den jetzt bekannten Ports aktualisieren.
        with self.lock:
            self.servers[self.server_id] = self.server_info("backup")

    def server_info(self, role: Optional[str] = None) -> Dict:
        """Erstellt eine kompakte 'Visitenkarte' dieses Servers (ID, Adresse,
        Ports, Rolle), die wir an andere verschicken."""
        return {
            "server_id": self.server_id,
            "host": self.host,
            "client_port": self.client_port,
            "server_port": self.server_port,
            "role": role or self.role,
        }

    def coordinator_info(self) -> Optional[Dict]:
        """Gibt die Infos zum aktuellen Coordinator zurück. Sind wir selbst der
        Coordinator, ist es unsere eigene Visitenkarte; sonst die des bekannten
        Coordinators (oder None, falls noch keiner feststeht)."""
        if self.role == "coordinator":
            return self.server_info("coordinator")
        return self.coordinator

    def log(self, text: str) -> None:
        """Gibt eine Statuszeile aus, mit Server-ID und Rolle davor.
        'flush=True' sorgt dafür, dass die Ausgabe sofort sichtbar wird."""
        print(f"[server {self.server_id} | {self.role}] {text}", flush=True)

    def start(self) -> None:
        """Startet den Server: Sockets binden, alle Hintergrund-Aufgaben (Threads)
        starten, sich ankündigen und die erste Rolle (Coordinator/Backup) klären."""
        self.bind_sockets()

        # Jede dieser Aufgaben läuft parallel in einem eigenen Thread:
        threading.Thread(target=self.discovery_listener, daemon=True).start()    # Suchanfragen beantworten.
        threading.Thread(target=self.server_listener, daemon=True).start()       # Server-Nachrichten empfangen.
        threading.Thread(target=self.client_listener, daemon=True).start()       # Client-Verbindungen annehmen.
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()        # "Ich lebe noch" senden.
        threading.Thread(target=self.state_sync_loop, daemon=True).start()       # Daten zu Backups spiegeln.
        threading.Thread(target=self.failure_detector_loop, daemon=True).start() # Coordinator-Ausfall erkennen.

        time.sleep(0.4)                  # Kurz warten, damit die Threads bereit sind.
        self.announce_server()           # Anderen Servern "Hallo" sagen.
        time.sleep(1.0)                  # Kurz warten, um andere Server kennenzulernen.
        self.decide_initial_coordinator()   # Erste Rolle bestimmen.
        self.log(f"started on client_port={self.client_port}, server_port={self.server_port}")

        # Hauptschleife: einfach am Leben bleiben, bis der Benutzer abbricht.
        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:        # Strg+C beendet den Server.
            self.running = False
            self.log("shutdown requested")

    # ------------------------------------------------------------------ #
    #  Multicast (Server finden sich gegenseitig / Coordinator-Ankündigung)
    # ------------------------------------------------------------------ #

    def create_multicast_socket(self) -> socket.socket:
        """Erstellt ein Socket, das an der Multicast-Gruppe 'teilnimmt', also
        Nachrichten empfängt, die an die Gruppenadresse gehen. Dieses Socket
        empfängt zugleich auch die Broadcasts der Clients (gleicher Port)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", self.discovery_port))                 # Auf allen Adressen lauschen.
        except OSError:
            sock.bind((self.multicast_group, self.discovery_port))
        # Der Multicast-Gruppe beitreten (auf allen Netzwerkkarten).
        mreq = struct.pack("4sl", socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return sock

    def multicast(self, message: Dict) -> None:
        """Sendet eine Nachricht an die Multicast-Gruppe (alle teilnehmenden Server).
        TTL=1 bedeutet: Das Paket bleibt im lokalen Netz und wird nicht weitergeroutet."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.sendto(encode(message), (self.multicast_group, self.discovery_port))
        sock.close()

    def discovery_listener(self) -> None:
        """Läuft dauerhaft und beantwortet Suchanfragen.

        - DISCOVERY_REQUEST (von einem Client): Wir antworten mit Infos zum
          aktuellen Coordinator, damit der Client sich verbinden kann.
        - SERVER_ANNOUNCE (von einem anderen Server): Wir merken ihn uns."""
        sock = self.create_multicast_socket()
        while self.running:
            try:
                data, address = sock.recvfrom(BUFFER_SIZE)   # Auf ein Paket warten.
                message = decode(data)
                msg_type = message.get("type")
                payload = message.get("payload", {})

                if msg_type == DISCOVERY_REQUEST:
                    # Direkt an den Absender (den Client) zurücksenden.
                    response = make_message(
                        DISCOVERY_RESPONSE,
                        coordinator=self.coordinator_info(),
                    )
                    sock.sendto(encode(response), address)

                elif msg_type == SERVER_ANNOUNCE:
                    self.handle_server_announce(payload)

            except Exception as exc:
                self.log(f"discovery listener error: {exc}")

    def announce_server(self) -> None:
        """Kündigt diesen Server bei allen anderen an ('Hallo, es gibt mich')."""
        self.multicast(make_message(SERVER_ANNOUNCE, server=self.server_info()))

    # ------------------------------------------------------------------ #
    #  Server-zu-Server-Kommunikation (Heartbeat, Wahl, Daten-Sync)
    # ------------------------------------------------------------------ #

    def server_listener(self) -> None:
        """Empfängt alle Nachrichten von anderen Servern und reicht sie je nach
        Typ an die passende Funktion weiter."""
        sock = self.server_sock
        while self.running:
            try:
                data, address = sock.recvfrom(BUFFER_SIZE)
                message = decode(data)
                payload = message.get("payload", {})
                msg_type = message.get("type")

                if msg_type == HEARTBEAT:
                    self.handle_heartbeat(payload)          # "Coordinator lebt noch."
                elif msg_type == STATE_SYNC:
                    self.handle_state_sync(payload)         # Frische Datenkopie erhalten.
                elif msg_type == ELECTION:
                    self.handle_election(payload, address, sock)   # Jemand will Coordinator werden.
                elif msg_type == ELECTION_OK:
                    # Ein höherer Server hat geantwortet -> wir gewinnen die Wahl nicht.
                    self.election_in_progress = False
                elif msg_type == COORDINATOR_ANNOUNCE:
                    self.handle_coordinator_announce(payload)  # Neuer Coordinator steht fest.
            except Exception as exc:
                self.log(f"server listener error: {exc}")

    def send_udp_to_server(self, server: Dict, message: Dict) -> None:
        """Schickt eine einzelne UDP-Nachricht gezielt an einen bestimmten Server."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(encode(message), (server["host"], int(server["server_port"])))
        sock.close()

    def handle_server_announce(self, payload: Dict) -> None:
        """Verarbeitet die 'Hallo'-Nachricht eines anderen Servers: ihn in die
        Liste der bekannten Server aufnehmen. Sind wir der Coordinator, schicken
        wir ihm gleich unseren aktuellen Datenstand."""
        server = payload.get("server")
        if not server:
            return
        sid = int(server["server_id"])
        if sid == self.server_id:        # Unsere eigene Ankündigung ignorieren.
            return
        with self.lock:
            self.servers[sid] = server
        if self.role == "coordinator":
            self.log(f"registered backup server {sid}")
            self.send_state_sync(server)

    def decide_initial_coordinator(self) -> None:
        """Bestimmt nach dem Start die erste Rolle: Haben wir die höchste ID
        unter den bekannten Servern, werden wir Coordinator. Sonst starten wir
        (falls noch kein Coordinator bekannt ist) eine Wahl."""
        with self.lock:
            highest_id = max(self.servers.keys())
        if self.server_id == highest_id:
            self.become_coordinator()
        elif self.coordinator is None:
            self.start_election()

    def become_coordinator(self) -> None:
        """Macht diesen Server zum Coordinator und teilt das allen anderen mit."""
        with self.lock:
            self.role = "coordinator"
            self.coordinator = self.server_info("coordinator")
            self.servers[self.server_id] = self.coordinator
            self.election_in_progress = False
        self.log("became coordinator")
        self.announce_coordinator()

    def announce_coordinator(self) -> None:
        """Teilt allen bekannten Servern gezielt per Unicast mit: 'Ich bin jetzt
        der Coordinator.' Unicast ist hier richtig, weil die Empfänger (die
        Backups) bereits bekannt sind - das ist zuverlässiger als Multicast."""
        msg = make_message(COORDINATOR_ANNOUNCE, coordinator=self.server_info("coordinator"))
        with self.lock:
            targets = list(self.servers.values())
        for server in targets:
            if int(server["server_id"]) != self.server_id:
                self.send_udp_to_server(server, msg)

    def handle_coordinator_announce(self, payload: Dict) -> None:
        """Reagiert auf die Ankündigung eines (neuen) Coordinators."""
        coord = payload.get("coordinator")
        if not coord:
            return
        coord_id = int(coord["server_id"])

        # Bully-Regel: Ein Server mit NIEDRIGERER ID darf nicht unser Coordinator
        # werden. Statt uns unterzuordnen, setzen wir unsere eigene (höhere)
        # Kandidatur durch, indem wir selbst eine Wahl starten. Das verhindert,
        # dass sich zwei Server gegenseitig als Coordinator überschreiben.
        if coord_id < self.server_id:
            self.log(f"rejected coordinator {coord_id} (lower id) - asserting own candidacy")
            self.start_election()
            return

        # Der neue Coordinator hat eine höhere (oder unsere) ID -> akzeptieren.
        with self.lock:
            self.servers[coord_id] = coord
            if coord_id != self.server_id:
                self.coordinator = coord
                self.role = "backup"
            self.election_in_progress = False
            self.last_heartbeat = time.time()
        if coord_id != self.server_id:
            self.log(f"accepted coordinator {coord_id}")

    # ------------------------------------------------------------------ #
    #  Client-Bedienung (nur der Coordinator macht das aktiv)
    # ------------------------------------------------------------------ #

    def client_listener(self) -> None:
        """Nimmt neue Client-Verbindungen an. Für jede Verbindung wird ein
        eigener Thread gestartet, damit mehrere Clients parallel bedient werden."""
        sock = self.client_sock
        while self.running:
            conn, address = sock.accept()    # Wartet auf einen neuen Client.
            threading.Thread(target=self.handle_client_connection, args=(conn, address), daemon=True).start()

    def handle_client_connection(self, conn: socket.socket, address: Tuple[str, int]) -> None:
        """Bedient einen einzelnen verbundenen Client, solange die Verbindung hält."""
        client_id = None
        try:
            for message in read_json_lines(conn):
                payload = message.get("payload", {})
                msg_type = message.get("type")

                # Nur der Coordinator darf Clients bedienen. Sind wir 'nur' ein
                # Backup, schicken wir den Client weiter (REDIRECT) und trennen.
                if self.role != "coordinator":
                    send_json_tcp(conn, make_message(REDIRECT))
                    conn.close()
                    return

                if msg_type == JOIN_REQUEST:
                    # Client möchte beitreten.
                    client_id = payload.get("client_id") or new_id("client")
                    username = payload.get("username", client_id)
                    room = payload.get("room", "general")
                    self.register_client(client_id, username, room, conn, address)

                elif msg_type == CHAT_MESSAGE:
                    # Client hat etwas geschrieben.
                    if client_id is None:
                        send_json_tcp(conn, make_message(ERROR, reason="client must join first"))
                    else:
                        self.order_and_distribute_message(
                            client_id, payload.get("text", ""), payload.get("msg_id")
                        )

                elif msg_type == LEAVE:
                    break                    # Client verabschiedet sich.
        except Exception as exc:
            self.log(f"client connection error: {exc}")
        finally:
            # Aufräumen: Client abmelden und Verbindung schliessen.
            if client_id:
                self.unregister_client(client_id)
            try:
                conn.close()
            except Exception:
                pass

    def register_client(self, client_id: str, username: str, room: str, conn: socket.socket, address: Tuple[str, int]) -> None:
        """Meldet einen Client an: in die Listen eintragen, Beitritt bestätigen
        (inkl. Teilnehmer und letzter Nachrichten) und die anderen informieren."""
        with self.lock:
            # Ist die client_id bereits bekannt, handelt es sich um einen
            # Reconnect (der neue Coordinator kennt den Client aus dem State-Sync).
            # Dann KEINE erneute "joined"-Meldung ausgeben.
            is_reconnect = client_id in self.clients
            self.clients[client_id] = {
                "client_id": client_id,
                "username": username,
                "room": room,
                "address": f"{address[0]}:{address[1]}",
                "joined_at_ms": now_ms(),
            }
            self.rooms.setdefault(room, set()).add(client_id)   # Raum anlegen/erweitern.
            self.client_connections[client_id] = conn
            participants = [self.clients[c]["username"] for c in self.rooms[room]]
            # Die letzten 20 Nachrichten dieses Raums für den Neuankömmling.
            history = [m for m in self.message_history if m["room"] == room][-20:]

        # Beitritt bestätigen.
        send_json_tcp(conn, make_message(
            JOIN_ACCEPTED,
            client_id=client_id,
            room=room,
            participants=participants,
            recent_messages=history,
        ))
        self.log(f"registered client {username} in room {room}")
        self.sync_state_to_all_backups()                         # Backups aktualisieren.
        # Nur bei echtem Erstbeitritt allen im Raum Bescheid geben.
        if not is_reconnect:
            self.order_and_distribute_system_message(room, f"{username} joined the room")

    def unregister_client(self, client_id: str) -> None:
        """Meldet einen Client wieder ab (Verbindung beendet/verlassen)."""
        with self.lock:
            client = self.clients.pop(client_id, None)
            self.client_connections.pop(client_id, None)
            if not client:
                return
            room = client["room"]
            self.rooms.get(room, set()).discard(client_id)
            username = client["username"]
        self.log(f"removed client {username}")
        self.sync_state_to_all_backups()
        self.order_and_distribute_system_message(room, f"{username} left the room")

    # ------------------------------------------------------------------ #
    #  Nachrichten ordnen und verteilen
    # ------------------------------------------------------------------ #

    def order_and_distribute_system_message(self, room: str, text: str) -> None:
        """Erzeugt eine System-Nachricht (z.B. 'X joined the room'). Absender ist
        'system'. System-Nachrichten haben keine msg_id (kein Dedup nötig)."""
        self._append_and_distribute(room, "system", "system", text)

    def order_and_distribute_message(self, client_id: str, text: str, msg_id: Optional[str] = None) -> None:
        """Verarbeitet eine Chat-Nachricht eines Clients - mit Doppel-Schutz.

        Dedup (Deduplizierung): Wurde genau diese Nachricht (gleiche msg_id) schon
        einmal geordnet - etwa, weil der Client sie nach einem Failover erneut
        gesendet hat -, ordnen wir sie NICHT noch einmal ein. Wir bestätigen dem
        Absender nur erneut die bereits vorhandene Nachricht. So gibt es keine
        Doppel-Einträge."""
        if msg_id:
            with self.lock:
                existing = self.seen_msg_ids.get(msg_id)
                conn = self.client_connections.get(client_id)
            if existing is not None:
                if conn:
                    try:
                        send_json_tcp(conn, make_message(ORDERED_MESSAGE, message=existing))
                    except Exception:
                        pass
                return

        # Neue, noch unbekannte Nachricht -> normal einordnen und verteilen.
        with self.lock:
            client = self.clients[client_id]
        self._append_and_distribute(client["room"], client_id, client["username"], text, msg_id)

    def _append_and_distribute(self, room: str, sender_id: str, sender_name: str,
                               text: str, msg_id: Optional[str] = None) -> None:
        """Das Herz der Nachrichten-Verteilung (nur der Coordinator macht das):
        1. Eine fortlaufende Nummer ('sequence') vergeben -> globale Reihenfolge.
        2. In die Historie aufnehmen (und ggf. für Dedup merken).
        3. An alle Clients im Raum senden.
        4. Die Backups auf den neuen Stand bringen."""
        with self.lock:
            self.global_sequence += 1
            ordered = {
                "sequence": self.global_sequence,   # Die Reihenfolge-Nummer.
                "room": room,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "text": text,
                "timestamp_ms": now_ms(),
                "msg_id": msg_id,                   # Vom Client vergebene ID (oder None).
            }
            self.message_history.append(ordered)
            if msg_id:
                self.seen_msg_ids[msg_id] = ordered  # Für späteres Dedup merken.
            recipients = list(self.rooms.get(room, set()))

        # Die fertige, nummerierte Nachricht an alle im Raum schicken.
        msg = make_message(ORDERED_MESSAGE, message=ordered)
        for cid in recipients:
            conn = self.client_connections.get(cid)
            if conn:
                try:
                    send_json_tcp(conn, msg)
                except Exception:
                    pass                            # Einzelner Fehler stoppt nicht die Verteilung.
        self.sync_state_to_all_backups()

    # ------------------------------------------------------------------ #
    #  Datenspiegelung (State-Sync) zu den Backups
    # ------------------------------------------------------------------ #

    def build_state_snapshot(self) -> Dict:
        """Erstellt eine Momentaufnahme ('Snapshot') des gesamten Zustands, den
        ein Backup braucht, um später übernehmen zu können. Mengen (sets)
        werden zu Listen, weil sich nur Listen ins Nachrichtenformat (JSON)
        umwandeln lassen."""
        with self.lock:
            return {
                "coordinator": self.coordinator_info(),
                "servers": self.servers,
                "clients": self.clients,
                "rooms": {room: list(members) for room, members in self.rooms.items()},
                "message_history": self.message_history[-100:],   # Nur die letzten 100.
                "global_sequence": self.global_sequence,
            }

    def handle_state_sync(self, payload: Dict) -> None:
        """Ein Backup erhält vom Coordinator einen Snapshot und übernimmt ihn
        als seinen eigenen Stand. So ist es jederzeit bereit, einzuspringen."""
        snapshot = payload.get("state")
        if not snapshot:
            return
        with self.lock:
            self.coordinator = snapshot.get("coordinator")
            # Schlüssel kamen als Text an -> zurück in Zahlen wandeln.
            self.servers = {int(k): v for k, v in snapshot.get("servers", {}).items()}
            self.clients = snapshot.get("clients", {})
            self.rooms = {room: set(members) for room, members in snapshot.get("rooms", {}).items()}
            self.message_history = snapshot.get("message_history", [])
            self.global_sequence = int(snapshot.get("global_sequence", 0))
            # Dedup-Tabelle aus der mitgelieferten Historie wiederherstellen,
            # damit ein späterer neuer Coordinator erneute Sendungen erkennt.
            self.seen_msg_ids = {
                m["msg_id"]: m for m in self.message_history if m.get("msg_id")
            }
        self.last_heartbeat = time.time()

    def send_state_sync(self, server: Dict) -> None:
        """Schickt einem einzelnen Backup den aktuellen Snapshot."""
        self.send_udp_to_server(server, make_message(STATE_SYNC, state=self.build_state_snapshot()))

    def sync_state_to_all_backups(self) -> None:
        """Schickt den aktuellen Snapshot an alle Backups (nur als Coordinator)."""
        if self.role != "coordinator":
            return
        with self.lock:
            backups = [s for sid, s in self.servers.items() if sid != self.server_id]
        for server in backups:
            self.send_state_sync(server)

    # ------------------------------------------------------------------ #
    #  Heartbeat (Lebenszeichen) und Ausfallerkennung
    # ------------------------------------------------------------------ #

    def heartbeat_loop(self) -> None:
        """Läuft dauerhaft. Sind wir der Coordinator, senden wir regelmässig ein
        Lebenszeichen an alle Backups, damit die wissen: 'Chef lebt noch.'"""
        while self.running:
            if self.role == "coordinator":
                with self.lock:
                    backups = [s for sid, s in self.servers.items() if sid != self.server_id]
                heartbeat = make_message(HEARTBEAT, coordinator=self.server_info("coordinator"))
                for server in backups:
                    self.send_udp_to_server(server, heartbeat)
            time.sleep(HEARTBEAT_INTERVAL)

    def handle_heartbeat(self, payload: Dict) -> None:
        """Ein Backup empfängt das Lebenszeichen des Coordinators und merkt sich
        den Zeitpunkt. Bleibt es zu lange aus, wird eine Wahl ausgelöst."""
        coord = payload.get("coordinator")
        if not coord:
            return
        coord_id = int(coord["server_id"])

        # Auch hier die Bully-Regel: Ein Lebenszeichen von einer NIEDRIGEREN ID
        # akzeptieren wir nicht als Coordinator, sondern setzen unsere eigene
        # (höhere) Kandidatur durch.
        if coord_id < self.server_id:
            self.start_election()
            return

        with self.lock:
            self.coordinator = coord
            self.servers[coord_id] = coord
            if coord_id != self.server_id:
                self.role = "backup"
            self.last_heartbeat = time.time()    # Uhr zurücksetzen: Chef lebt.

    def state_sync_loop(self) -> None:
        """Läuft dauerhaft. Als Coordinator spiegeln wir regelmässig (auch ohne
        neue Nachrichten) unseren Stand zu den Backups, damit sie aktuell bleiben."""
        while self.running:
            if self.role == "coordinator":
                self.sync_state_to_all_backups()
            time.sleep(STATE_SYNC_INTERVAL)

    def failure_detector_loop(self) -> None:
        """Läuft dauerhaft. Als Backup prüfen wir regelmässig, ob das letzte
        Lebenszeichen des Coordinators zu lange her ist. Wenn ja, gilt er als
        ausgefallen und wir starten eine Wahl."""
        while self.running:
            time.sleep(0.5)
            if self.role == "backup" and self.coordinator is not None:
                if time.time() - self.last_heartbeat > HEARTBEAT_TIMEOUT:
                    self.log("coordinator heartbeat missing. starting election")
                    self.start_election()
                    self.last_heartbeat = time.time()   # Uhr zurücksetzen, sonst Dauerwahl.

    # ------------------------------------------------------------------ #
    #  Wahl (Bully-Algorithmus)
    # ------------------------------------------------------------------ #

    def start_election(self) -> None:
        """Startet eine Wahl nach dem Bully-Prinzip.

        Idee: Wir fragen alle Server mit HÖHERER ID 'lebt ihr noch?'.
          - Antwortet niemand mit höherer ID, sind wir der Höchste -> wir werden
            Coordinator.
          - Antwortet ein Höherer (mit ELECTION_OK), übernimmt der die Wahl und
            wir warten ab."""
        with self.lock:
            if self.election_in_progress:    # Läuft schon eine Wahl -> nicht doppelt starten.
                return
            self.election_in_progress = True
            higher = [s for sid, s in self.servers.items() if sid > self.server_id]
        if not higher:
            self.become_coordinator()        # Niemand Höheres da -> wir gewinnen.
            return
        # An alle Höheren eine ELECTION-Nachricht schicken.
        msg = make_message(ELECTION, candidate=self.server_info())
        for server in higher:
            self.send_udp_to_server(server, msg)
        # Auf Antworten warten - zeitlich begrenzt, in einem eigenen Thread.
        threading.Thread(target=self.election_timeout, daemon=True).start()

    def election_timeout(self) -> None:
        """Wartet kurz auf Wahl-Antworten. Hat sich bis dahin kein höherer Server
        gemeldet (election_in_progress ist noch True), werden wir Coordinator."""
        time.sleep(2.0)
        with self.lock:
            still_waiting = self.election_in_progress
        if still_waiting:
            self.become_coordinator()

    def handle_election(self, payload: Dict, address: Tuple[str, int], sock: socket.socket) -> None:
        """Wir bekommen eine Wahl-Anfrage von einem anderen Server. Hat dieser
        eine NIEDRIGERE ID als wir, antworten wir mit ELECTION_OK ('stopp, ich
        bin höher') und starten selbst eine Wahl, um Coordinator zu werden."""
        candidate = payload.get("candidate", {})
        candidate_id = int(candidate.get("server_id", -1))
        if candidate_id < self.server_id:
            sock.sendto(encode(make_message(ELECTION_OK)), address)
            self.start_election()


def generate_server_id() -> int:
    """Erzeugt eine eindeutige, vergleichbare Server-ID - ohne dass sich die
    Server vorher absprechen müssen. Ein Zufallswert aus einem sehr grossen
    Bereich macht doppelte IDs äusserst unwahrscheinlich. Die höchste ID
    gewinnt später die Wahl."""
    return random.randint(1, 2_000_000_000)


def main() -> None:
    """Einstiegspunkt: liest die Kommandozeilen-Argumente und startet den Server.
    Ohne Angaben werden ID und Ports automatisch bestimmt - man kann also einfach
    'python server.py' aufrufen (auch mehrfach für mehrere Server)."""
    parser = argparse.ArgumentParser(description="Distributed chat server with broadcast discovery and Bully election")
    parser.add_argument("--id", type=int, default=None,
                        help="Server-ID (Default: automatisch zufällig vergeben)")
    parser.add_argument("--host", default=None,
                        help="An Clients gemeldete Adresse (Default: automatisch ermittelte LAN-IP)")
    parser.add_argument("--client-port", type=int, default=0,
                        help="TCP-Port für Clients (Default: 0 = vom OS frei gewählt)")
    parser.add_argument("--server-port", type=int, default=0,
                        help="UDP-Port für Server-zu-Server (Default: 0 = vom OS frei gewählt)")
    parser.add_argument("--multicast-group", default=MULTICAST_GROUP)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)
    args = parser.parse_args()

    # Fehlt eine ID, eine zufällige erzeugen. Fehlt der Host, die LAN-IP ermitteln.
    server_id = args.id if args.id is not None else generate_server_id()
    host = args.host or detect_lan_ip()

    ChatServer(
        server_id=server_id,
        host=host,
        client_port=args.client_port,
        server_port=args.server_port,
        multicast_group=args.multicast_group,
        discovery_port=args.discovery_port,
    ).start()


# main() nur ausführen, wenn die Datei direkt gestartet wird (python server.py).
if __name__ == "__main__":
    main()
