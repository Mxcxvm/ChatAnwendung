import argparse
import ipaddress
import socket
import threading
import time
from typing import Dict, Tuple, Optional, Any

from protocol import *

BROADCAST_ADDRESS = "255.255.255.255"
DISCOVERY_PORT = 5973

HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.5
STATE_SYNC_INTERVAL = 2.0
SERVER_ANNOUNCE_INTERVAL = 3.0


class ChatServer:
    def __init__(
        self,
        server_id: int,
        host: str,
        client_port: int,
        server_port: int,
        broadcast_address: str = BROADCAST_ADDRESS,
        discovery_port: int = DISCOVERY_PORT,
    ):
        self.server_id = server_id
        self.host = host
        self.client_port = client_port
        self.server_port = server_port
        self.broadcast_address = broadcast_address
        self.discovery_port = discovery_port

        self.role = "backup"
        self.coordinator: Optional[Dict[str, Any]] = None

        self.servers: Dict[int, Dict[str, Any]] = {
            self.server_id: self.server_info("backup")
        }

        self.clients: Dict[str, Dict[str, Any]] = {}
        self.rooms: Dict[str, set] = {}
        self.message_history = []
        self.global_sequence = 0
        self.client_connections: Dict[str, socket.socket] = {}

        self.lock = threading.RLock()
        self.running = True
        self.last_heartbeat = time.time()
        self.election_in_progress = False

    def server_info(self, role: Optional[str] = None) -> Dict[str, Any]:
        return {
            "server_id": self.server_id,
            "host": self.host,
            "client_port": self.client_port,
            "server_port": self.server_port,
            "role": role or self.role,
        }

    def coordinator_info(self) -> Optional[Dict[str, Any]]:
        if self.role == "coordinator":
            return self.server_info("coordinator")

        return self.coordinator

    def log(self, text: str) -> None:
        print(f"[server {self.server_id} | {self.role}] {text}", flush=True)

    def start(self) -> None:
        threading.Thread(target=self.broadcast_listener, daemon=True).start()
        threading.Thread(target=self.client_listener, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.state_sync_loop, daemon=True).start()
        threading.Thread(target=self.failure_detector_loop, daemon=True).start()
        threading.Thread(target=self.server_announce_loop, daemon=True).start()

        time.sleep(0.5)
        self.announce_server()

        time.sleep(1.2)
        self.decide_initial_coordinator()

        self.log(
            f"started on host={self.host}, "
            f"client_port={self.client_port}, "
            f"server_port={self.server_port}, "
            f"broadcast={self.broadcast_address}:{self.discovery_port}"
        )

        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.running = False
            self.log("shutdown requested")

    def create_broadcast_listener_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        if hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(("", self.discovery_port))

        return sock

    def broadcast(self, message: Dict[str, Any]) -> None:
        payload = dict(message.get("payload", {}))
        payload.setdefault("sender_server_id", self.server_id)

        outgoing = {
            "type": message.get("type"),
            "timestamp_ms": message.get("timestamp_ms", now_ms()),
            "payload": payload,
        }

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.sendto(
                encode(outgoing),
                (self.broadcast_address, self.discovery_port),
            )
        finally:
            sock.close()

    def is_own_broadcast_message(self, payload: Dict[str, Any]) -> bool:
        sender_id = payload.get("sender_server_id")

        if sender_id is None:
            return False

        try:
            return int(sender_id) == self.server_id
        except (TypeError, ValueError):
            return False

    def valid_server_info(self, server: Optional[Dict[str, Any]]) -> bool:
        if not server:
            return False

        try:
            int(server["server_id"])
            host = str(server["host"])
            int(server["client_port"])
            int(server["server_port"])

            ip = ipaddress.ip_address(socket.gethostbyname(host))
        except Exception:
            return False

        if ip.is_unspecified or ip.is_multicast:
            return False

        return True

    def broadcast_listener(self) -> None:
        sock = self.create_broadcast_listener_socket()
        self.log("broadcast listener ready")

        while self.running:
            try:
                data, address = sock.recvfrom(BUFFER_SIZE)
                message = decode(data)

                msg_type = message.get("type")
                payload = message.get("payload", {})

                if msg_type == DISCOVERY_REQUEST:
                    response = make_message(
                        DISCOVERY_RESPONSE,
                        responding_server=self.server_info(),
                        coordinator=self.coordinator_info(),
                    )
                    sock.sendto(encode(response), address)
                    continue

                if self.is_own_broadcast_message(payload):
                    continue

                if msg_type == SERVER_ANNOUNCE:
                    self.handle_server_announce(payload)

                elif msg_type == COORDINATOR_ANNOUNCE:
                    self.handle_coordinator_announce(payload)

                elif msg_type == HEARTBEAT:
                    self.handle_heartbeat(payload)

                elif msg_type == STATE_SYNC:
                    self.handle_state_sync(payload)

                elif msg_type == ELECTION:
                    self.handle_election(payload)

                elif msg_type == ELECTION_OK:
                    self.handle_election_ok(payload)

            except Exception as exc:
                self.log(f"broadcast listener error: {exc}")

    def announce_server(self) -> None:
        self.broadcast(make_message(
            SERVER_ANNOUNCE,
            server=self.server_info(),
        ))

    def server_announce_loop(self) -> None:
        while self.running:
            time.sleep(SERVER_ANNOUNCE_INTERVAL)

            self.announce_server()

            if self.role == "coordinator":
                self.announce_coordinator()

    def handle_server_announce(self, payload: Dict[str, Any]) -> None:
        server = payload.get("server")

        if not self.valid_server_info(server):
            self.log(f"ignored invalid server announcement: {server}")
            return

        sid = int(server["server_id"])

        if sid == self.server_id:
            return

        with self.lock:
            was_known = sid in self.servers
            self.servers[sid] = server

        if not was_known:
            self.log(
                f"discovered server {sid} at "
                f"{server['host']}:{server['server_port']}"
            )

        if self.role == "coordinator":
            self.sync_state_to_all_backups()
            self.announce_coordinator()

    def decide_initial_coordinator(self) -> None:
        with self.lock:
            highest_id = max(self.servers.keys())

        if self.server_id == highest_id:
            self.become_coordinator()
        elif self.coordinator is None:
            self.start_election()

    def become_coordinator(self) -> None:
        with self.lock:
            if self.role == "coordinator" and self.coordinator is not None:
                return

            self.role = "coordinator"
            self.coordinator = self.server_info("coordinator")
            self.servers[self.server_id] = self.coordinator
            self.election_in_progress = False
            self.last_heartbeat = time.time()

        self.log("became coordinator")
        self.announce_coordinator()
        self.sync_state_to_all_backups()

    def announce_coordinator(self) -> None:
        self.broadcast(make_message(
            COORDINATOR_ANNOUNCE,
            coordinator=self.server_info("coordinator"),
        ))

    def handle_coordinator_announce(self, payload: Dict[str, Any]) -> None:
        coord = payload.get("coordinator")

        if not self.valid_server_info(coord):
            self.log(f"ignored invalid coordinator announcement: {coord}")
            return

        coord_id = int(coord["server_id"])

        if coord_id == self.server_id:
            return

        with self.lock:
            if self.role == "coordinator" and self.server_id > coord_id:
                should_reannounce = True
            else:
                should_reannounce = False

        if should_reannounce:
            self.announce_coordinator()
            return

        with self.lock:
            self.coordinator = coord
            self.servers[coord_id] = coord
            self.role = "backup"
            self.election_in_progress = False
            self.last_heartbeat = time.time()

        self.log(f"accepted coordinator {coord_id}")

    def client_listener(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.client_port))
        sock.listen(50)

        self.log(f"client listener ready on 0.0.0.0:{self.client_port}")

        while self.running:
            try:
                conn, address = sock.accept()

                threading.Thread(
                    target=self.handle_client_connection,
                    args=(conn, address),
                    daemon=True,
                ).start()

            except OSError as exc:
                self.log(f"client listener error: {exc}")

    def handle_client_connection(
        self,
        conn: socket.socket,
        address: Tuple[str, int],
    ) -> None:
        client_id = None

        try:
            for message in read_json_lines(conn):
                payload = message.get("payload", {})
                msg_type = message.get("type")

                if self.role != "coordinator":
                    coord = self.coordinator_info()

                    if coord:
                        send_json_tcp(conn, make_message(
                            REDIRECT,
                            coordinator=coord,
                        ))
                    else:
                        send_json_tcp(conn, make_message(
                            ERROR,
                            reason="coordinator not known yet",
                        ))

                    conn.close()
                    return

                if msg_type == JOIN_REQUEST:
                    client_id = payload.get("client_id") or new_id("client")
                    username = payload.get("username", client_id)
                    room = payload.get("room", "general")

                    self.register_client(
                        client_id,
                        username,
                        room,
                        conn,
                        address,
                    )

                elif msg_type == CHAT_MESSAGE:
                    if client_id is None:
                        send_json_tcp(conn, make_message(
                            ERROR,
                            reason="client must join first",
                        ))
                    else:
                        self.order_and_distribute_message(
                            client_id,
                            payload.get("text", ""),
                        )

                elif msg_type == LEAVE:
                    break

        except Exception as exc:
            self.log(f"client connection error: {exc}")

        finally:
            if client_id:
                self.unregister_client(client_id)

            try:
                conn.close()
            except Exception:
                pass

    def register_client(
        self,
        client_id: str,
        username: str,
        room: str,
        conn: socket.socket,
        address: Tuple[str, int],
    ) -> None:
        with self.lock:
            is_rejoin = client_id in self.clients

            self.clients[client_id] = {
                "client_id": client_id,
                "username": username,
                "room": room,
                "address": f"{address[0]}:{address[1]}",
                "joined_at_ms": now_ms(),
            }

            self.rooms.setdefault(room, set()).add(client_id)
            self.client_connections[client_id] = conn

            participants = [
                self.clients[c]["username"]
                for c in self.rooms[room]
            ]

            history = [
                m for m in self.message_history
                if m["room"] == room
            ][-20:]

        send_json_tcp(conn, make_message(
            JOIN_ACCEPTED,
            client_id=client_id,
            room=room,
            participants=participants,
            recent_messages=history,
            coordinator=self.server_info("coordinator"),
        ))

        self.log(
            f"{'re' if is_rejoin else ''}"
            f"registered client {username} in room {room}"
        )

        self.sync_state_to_all_backups()

        if not is_rejoin:
            self.order_and_distribute_system_message(
                room,
                f"{username} joined the room",
            )

    def unregister_client(self, client_id: str) -> None:
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
        self.order_and_distribute_system_message(
            room,
            f"{username} left the room",
        )

    def order_and_distribute_system_message(
        self,
        room: str,
        text: str,
    ) -> None:
        self._append_and_distribute(
            room,
            "system",
            "system",
            text,
        )

    def order_and_distribute_message(
        self,
        client_id: str,
        text: str,
    ) -> None:
        with self.lock:
            client = self.clients[client_id]

        self._append_and_distribute(
            client["room"],
            client_id,
            client["username"],
            text,
        )

    def _append_and_distribute(
        self,
        room: str,
        sender_id: str,
        sender_name: str,
        text: str,
    ) -> None:
        with self.lock:
            self.global_sequence += 1

            ordered = {
                "sequence": self.global_sequence,
                "room": room,
                "sender_id": sender_id,
                "sender_name": sender_name,
                "text": text,
                "timestamp_ms": now_ms(),
            }

            self.message_history.append(ordered)
            recipients = list(self.rooms.get(room, set()))

        msg = make_message(ORDERED_MESSAGE, message=ordered)

        for cid in recipients:
            conn = self.client_connections.get(cid)

            if conn:
                try:
                    send_json_tcp(conn, msg)
                except Exception:
                    pass

        self.sync_state_to_all_backups()

    def build_state_snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return {
                "coordinator": self.coordinator_info(),
                "servers": self.servers,
                "clients": self.clients,
                "rooms": {
                    room: list(members)
                    for room, members in self.rooms.items()
                },
                "message_history": self.message_history[-100:],
                "global_sequence": self.global_sequence,
            }

    def handle_state_sync(self, payload: Dict[str, Any]) -> None:
        if self.role == "coordinator":
            return

        snapshot = payload.get("state")

        if not snapshot:
            return

        clean_servers: Dict[int, Dict[str, Any]] = {}

        for sid, server in snapshot.get("servers", {}).items():
            if self.valid_server_info(server):
                clean_servers[int(sid)] = server

        clean_servers[self.server_id] = self.server_info()

        coord = snapshot.get("coordinator")

        if not self.valid_server_info(coord):
            coord = self.coordinator

        with self.lock:
            self.coordinator = coord
            self.servers = clean_servers
            self.clients = snapshot.get("clients", {})
            self.rooms = {
                room: set(members)
                for room, members in snapshot.get("rooms", {}).items()
            }
            self.message_history = snapshot.get("message_history", [])
            self.global_sequence = int(snapshot.get("global_sequence", 0))
            self.last_heartbeat = time.time()

    def sync_state_to_all_backups(self) -> None:
        if self.role != "coordinator":
            return

        with self.lock:
            has_backup = any(
                sid != self.server_id
                for sid in self.servers.keys()
            )

        if has_backup:
            self.broadcast(make_message(
                STATE_SYNC,
                state=self.build_state_snapshot(),
            ))

    def send_heartbeat_to_clients(
        self,
        heartbeat: Dict[str, Any],
    ) -> None:
        with self.lock:
            connections = list(self.client_connections.items())

        dead_clients = []

        for client_id, conn in connections:
            try:
                send_json_tcp(conn, heartbeat)
            except Exception:
                dead_clients.append(client_id)

        for client_id in dead_clients:
            self.unregister_client(client_id)

    def heartbeat_loop(self) -> None:
        while self.running:
            if self.role == "coordinator":
                heartbeat = make_message(
                    HEARTBEAT,
                    coordinator=self.server_info("coordinator"),
                )

                self.broadcast(heartbeat)
                self.send_heartbeat_to_clients(heartbeat)

            time.sleep(HEARTBEAT_INTERVAL)

    def handle_heartbeat(self, payload: Dict[str, Any]) -> None:
        coord = payload.get("coordinator")

        if not self.valid_server_info(coord):
            return

        coord_id = int(coord["server_id"])

        if coord_id == self.server_id:
            return

        with self.lock:
            if self.role == "coordinator" and self.server_id > coord_id:
                should_reannounce = True
            else:
                should_reannounce = False

        if should_reannounce:
            self.announce_coordinator()
            return

        with self.lock:
            self.coordinator = coord
            self.servers[coord_id] = coord
            self.role = "backup"
            self.election_in_progress = False
            self.last_heartbeat = time.time()

    def state_sync_loop(self) -> None:
        while self.running:
            if self.role == "coordinator":
                self.sync_state_to_all_backups()

            time.sleep(STATE_SYNC_INTERVAL)

    def failure_detector_loop(self) -> None:
        while self.running:
            time.sleep(0.5)

            if self.role == "backup" and self.coordinator is not None:
                if time.time() - self.last_heartbeat > HEARTBEAT_TIMEOUT:
                    with self.lock:
                        dead_coordinator = self.coordinator

                        if dead_coordinator:
                            try:
                                dead_id = int(dead_coordinator["server_id"])

                                if dead_id != self.server_id:
                                    self.servers.pop(dead_id, None)

                            except Exception:
                                pass

                        self.coordinator = None

                    self.log("coordinator heartbeat missing. starting election")
                    self.start_election()
                    self.last_heartbeat = time.time()

    def start_election(self) -> None:
        with self.lock:
            if self.election_in_progress:
                return

            self.election_in_progress = True

            higher = [
                sid for sid in self.servers.keys()
                if sid > self.server_id
            ]

        if not higher:
            self.become_coordinator()
            return

        self.log(f"starting election; higher servers known: {higher}")

        self.broadcast(make_message(
            ELECTION,
            candidate=self.server_info(),
        ))

        threading.Thread(
            target=self.election_timeout,
            daemon=True,
        ).start()

    def election_timeout(self) -> None:
        time.sleep(2.0)

        with self.lock:
            still_waiting = self.election_in_progress

        if still_waiting:
            self.become_coordinator()

    def handle_election(self, payload: Dict[str, Any]) -> None:
        candidate = payload.get("candidate", {})

        if not self.valid_server_info(candidate):
            return

        candidate_id = int(candidate["server_id"])

        if candidate_id == self.server_id:
            return

        with self.lock:
            self.servers[candidate_id] = candidate

        if candidate_id < self.server_id:
            self.broadcast(make_message(
                ELECTION_OK,
                target_server_id=candidate_id,
                responder=self.server_info(),
            ))

            self.start_election()

    def handle_election_ok(self, payload: Dict[str, Any]) -> None:
        try:
            target_server_id = int(payload.get("target_server_id"))
        except (TypeError, ValueError):
            return

        if target_server_id != self.server_id:
            return

        responder = payload.get("responder")

        if self.valid_server_info(responder):
            with self.lock:
                self.servers[int(responder["server_id"])] = responder
                self.election_in_progress = False
                self.last_heartbeat = time.time()

            self.log(f"received ELECTION_OK from {responder['server_id']}")


def derive_server_id(host: str, server_port: int) -> int:
    return int(ipaddress.ip_address(socket.gethostbyname(host))) * 100000 + server_port


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())


def ensure_local_ipv4(host: str) -> str:
    ip = socket.gethostbyname(host)
    parsed = ipaddress.ip_address(ip)

    if parsed.is_unspecified or parsed.is_multicast:
        raise SystemExit(f"Ungültige Server-IP für --host: {ip}")

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((ip, 0))
    except OSError:
        raise SystemExit(
            f"Die IP {ip} ist auf diesem Rechner nicht als lokale IPv4-Adresse verfügbar. "
            f"Nutze die IP aus ipconfig oder lasse --host weg."
        )

    return ip


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Distributed chat server with UDP broadcast discovery and Bully election"
    )

    parser.add_argument("--host", default=None)
    parser.add_argument("--client-port", type=int, default=None)
    parser.add_argument("--server-port", type=int, default=None)

    parser.add_argument("--broadcast-address", default=BROADCAST_ADDRESS)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)

    args = parser.parse_args()

    host = ensure_local_ipv4(args.host) if args.host else get_local_ip()
    client_port = args.client_port or find_free_port()
    server_port = args.server_port or find_free_port()
    server_id = derive_server_id(host, server_port)

    ChatServer(
        server_id=server_id,
        host=host,
        client_port=client_port,
        server_port=server_port,
        broadcast_address=args.broadcast_address,
        discovery_port=args.discovery_port,
    ).start()


if __name__ == "__main__":
    main()