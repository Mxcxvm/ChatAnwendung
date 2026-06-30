import argparse
import ipaddress
import socket
import struct
import threading
import time
from typing import Dict, Tuple, Optional

from protocol import *

MULTICAST_GROUP = "224.1.1.1"
DISCOVERY_PORT = 5973
HEARTBEAT_INTERVAL = 1.0
HEARTBEAT_TIMEOUT = 3.2
STATE_SYNC_INTERVAL = 2.0


class ChatServer:
    def __init__(self, server_id: int, host: str, client_port: int, server_port: int,
                 multicast_group: str = MULTICAST_GROUP, discovery_port: int = DISCOVERY_PORT):
        self.server_id = server_id
        self.host = host
        self.client_port = client_port
        self.server_port = server_port
        self.multicast_group = multicast_group
        self.discovery_port = discovery_port

        self.role = "backup"
        self.coordinator: Optional[Dict] = None
        self.servers: Dict[int, Dict] = {
            self.server_id: self.server_info("backup")
        }
        self.clients: Dict[str, Dict] = {}
        self.rooms: Dict[str, set] = {}
        self.message_history = []
        self.global_sequence = 0
        self.client_connections: Dict[str, socket.socket] = {}

        self.lock = threading.RLock()
        self.running = True
        self.last_heartbeat = time.time()
        self.election_in_progress = False

    def server_info(self, role: Optional[str] = None) -> Dict:
        return {
            "server_id": self.server_id,
            "host": self.host,
            "client_port": self.client_port,
            "server_port": self.server_port,
            "role": role or self.role,
        }

    def coordinator_info(self) -> Optional[Dict]:
        if self.role == "coordinator":
            return self.server_info("coordinator")
        return self.coordinator

    def log(self, text: str) -> None:
        print(f"[server {self.server_id} | {self.role}] {text}", flush=True)

    def start(self) -> None:
        threading.Thread(target=self.discovery_listener, daemon=True).start()
        threading.Thread(target=self.server_listener, daemon=True).start()
        threading.Thread(target=self.client_listener, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.state_sync_loop, daemon=True).start()
        threading.Thread(target=self.failure_detector_loop, daemon=True).start()

        time.sleep(0.4)
        self.announce_server()
        time.sleep(1.0)
        self.decide_initial_coordinator()
        self.log(f"started on client_port={self.client_port}, server_port={self.server_port}")

        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.running = False
            self.log("shutdown requested")

    def create_multicast_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", self.discovery_port))
        except OSError:
            sock.bind((self.multicast_group, self.discovery_port))
        mreq = struct.pack("4sl", socket.inet_aton(self.multicast_group), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return sock

    def multicast(self, message: Dict) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.sendto(encode(message), (self.multicast_group, self.discovery_port))
        sock.close()

    def discovery_listener(self) -> None:
        sock = self.create_multicast_socket()
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

                elif msg_type == SERVER_ANNOUNCE:
                    self.handle_server_announce(payload)

            except Exception as exc:
                self.log(f"discovery listener error: {exc}")

    def announce_server(self) -> None:
        self.multicast(make_message(SERVER_ANNOUNCE, server=self.server_info()))

    def server_listener(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.server_port))
        while self.running:
            try:
                data, address = sock.recvfrom(BUFFER_SIZE)
                message = decode(data)
                payload = message.get("payload", {})
                msg_type = message.get("type")

                if msg_type == SERVER_ANNOUNCE:
                    self.handle_server_announce(payload)
                elif msg_type == HEARTBEAT:
                    self.handle_heartbeat(payload)
                elif msg_type == STATE_SYNC:
                    self.handle_state_sync(payload)
                elif msg_type == ELECTION:
                    self.handle_election(payload, address, sock)
                elif msg_type == ELECTION_OK:
                    self.election_in_progress = False
                elif msg_type == COORDINATOR_ANNOUNCE:
                    self.handle_coordinator_announce(payload)
            except Exception as exc:
                self.log(f"server listener error: {exc}")

    def send_udp_to_server(self, server: Dict, message: Dict) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(encode(message), (server["host"], int(server["server_port"])))
        sock.close()

    def handle_server_announce(self, payload: Dict) -> None:
        server = payload.get("server")
        if not server:
            return
        sid = int(server["server_id"])
        if sid == self.server_id:
            return
        with self.lock:
            self.servers[sid] = server
        if self.role == "coordinator":
            self.log(f"registered backup server {sid}")
            self.send_state_sync(server)

    def decide_initial_coordinator(self) -> None:
        with self.lock:
            highest_id = max(self.servers.keys())
        if self.server_id == highest_id:
            self.become_coordinator()
        elif self.coordinator is None:
            self.start_election()

    def become_coordinator(self) -> None:
        with self.lock:
            self.role = "coordinator"
            self.coordinator = self.server_info("coordinator")
            self.servers[self.server_id] = self.coordinator
            self.election_in_progress = False
        self.log("became coordinator")
        self.announce_coordinator()

    def announce_coordinator(self) -> None:
        msg = make_message(COORDINATOR_ANNOUNCE, coordinator=self.server_info("coordinator"))
        with self.lock:
            targets = list(self.servers.values())
        for server in targets:
            if int(server["server_id"]) != self.server_id:
                self.send_udp_to_server(server, msg)
        self.multicast(msg)

    def handle_coordinator_announce(self, payload: Dict) -> None:
        coord = payload.get("coordinator")
        if not coord:
            return
        with self.lock:
            self.coordinator = coord
            self.servers[int(coord["server_id"])] = coord
            if int(coord["server_id"]) != self.server_id:
                self.role = "backup"
            self.election_in_progress = False
            self.last_heartbeat = time.time()
        self.log(f"accepted coordinator {coord['server_id']}")

    def client_listener(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.client_port))
        sock.listen(50)
        while self.running:
            conn, address = sock.accept()
            threading.Thread(target=self.handle_client_connection, args=(conn, address), daemon=True).start()

    def handle_client_connection(self, conn: socket.socket, address: Tuple[str, int]) -> None:
        client_id = None
        try:
            for message in read_json_lines(conn):
                payload = message.get("payload", {})
                msg_type = message.get("type")

                if self.role != "coordinator":
                    send_json_tcp(conn, make_message(REDIRECT, coordinator=self.coordinator_info()))
                    conn.close()
                    return

                if msg_type == JOIN_REQUEST:
                    client_id = payload.get("client_id") or new_id("client")
                    username = payload.get("username", client_id)
                    room = payload.get("room", "general")
                    self.register_client(client_id, username, room, conn, address)

                elif msg_type == CHAT_MESSAGE:
                    if client_id is None:
                        send_json_tcp(conn, make_message(ERROR, reason="client must join first"))
                    else:
                        self.order_and_distribute_message(client_id, payload.get("text", ""))

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

    def register_client(self, client_id: str, username: str, room: str, conn: socket.socket, address: Tuple[str, int]) -> None:
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
            participants = [self.clients[c]["username"] for c in self.rooms[room]]
            history = [m for m in self.message_history if m["room"] == room][-20:]

        send_json_tcp(conn, make_message(
            JOIN_ACCEPTED,
            client_id=client_id,
            room=room,
            participants=participants,
            recent_messages=history,
            coordinator=self.server_info("coordinator"),
        ))
        self.log(f"{'re' if is_rejoin else ''}registered client {username} in room {room}")
        self.sync_state_to_all_backups()
        if not is_rejoin:
            self.order_and_distribute_system_message(room, f"{username} joined the room")

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
        self.order_and_distribute_system_message(room, f"{username} left the room")

    def order_and_distribute_system_message(self, room: str, text: str) -> None:
        self._append_and_distribute(room, "system", "system", text)

    def order_and_distribute_message(self, client_id: str, text: str) -> None:
        with self.lock:
            client = self.clients[client_id]
        self._append_and_distribute(client["room"], client_id, client["username"], text)

    def _append_and_distribute(self, room: str, sender_id: str, sender_name: str, text: str) -> None:
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

    def build_state_snapshot(self) -> Dict:
        with self.lock:
            return {
                "coordinator": self.coordinator_info(),
                "servers": self.servers,
                "clients": self.clients,
                "rooms": {room: list(members) for room, members in self.rooms.items()},
                "message_history": self.message_history[-100:],
                "global_sequence": self.global_sequence,
            }

    def handle_state_sync(self, payload: Dict) -> None:
        snapshot = payload.get("state")
        if not snapshot:
            return
        with self.lock:
            self.coordinator = snapshot.get("coordinator")
            self.servers = {int(k): v for k, v in snapshot.get("servers", {}).items()}
            self.clients = snapshot.get("clients", {})
            self.rooms = {room: set(members) for room, members in snapshot.get("rooms", {}).items()}
            self.message_history = snapshot.get("message_history", [])
            self.global_sequence = int(snapshot.get("global_sequence", 0))
        self.last_heartbeat = time.time()

    def send_state_sync(self, server: Dict) -> None:
        self.send_udp_to_server(server, make_message(STATE_SYNC, state=self.build_state_snapshot()))

    def sync_state_to_all_backups(self) -> None:
        if self.role != "coordinator":
            return
        with self.lock:
            backups = [s for sid, s in self.servers.items() if sid != self.server_id]
        for server in backups:
            self.send_state_sync(server)

    def heartbeat_loop(self) -> None:
        while self.running:
            if self.role == "coordinator":
                with self.lock:
                    backups = [s for sid, s in self.servers.items() if sid != self.server_id]
                heartbeat = make_message(HEARTBEAT, coordinator=self.server_info("coordinator"))
                for server in backups:
                    self.send_udp_to_server(server, heartbeat)
            time.sleep(HEARTBEAT_INTERVAL)

    def handle_heartbeat(self, payload: Dict) -> None:
        coord = payload.get("coordinator")
        if coord:
            with self.lock:
                self.coordinator = coord
                self.servers[int(coord["server_id"])] = coord
                if int(coord["server_id"]) != self.server_id:
                    self.role = "backup"
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
                    self.log("coordinator heartbeat missing. starting election")
                    self.start_election()
                    self.last_heartbeat = time.time()

    def start_election(self) -> None:
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            higher = [s for sid, s in self.servers.items() if sid > self.server_id]
        if not higher:
            self.become_coordinator()
            return
        msg = make_message(ELECTION, candidate=self.server_info())
        for server in higher:
            self.send_udp_to_server(server, msg)
        threading.Thread(target=self.election_timeout, daemon=True).start()

    def election_timeout(self) -> None:
        time.sleep(2.0)
        with self.lock:
            still_waiting = self.election_in_progress
        if still_waiting:
            self.become_coordinator()

    def handle_election(self, payload: Dict, address: Tuple[str, int], sock: socket.socket) -> None:
        candidate = payload.get("candidate", {})
        candidate_id = int(candidate.get("server_id", -1))
        if candidate_id < self.server_id:
            sock.sendto(encode(make_message(ELECTION_OK, responder=self.server_info())), address)
            self.start_election()


def derive_server_id(host: str, server_port: int) -> int:
    return int(ipaddress.ip_address(socket.gethostbyname(host))) * 100000 + server_port


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def get_local_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed chat server with multicast discovery and Bully election")
    parser.add_argument("--host", default=None)
    parser.add_argument("--client-port", type=int, default=None)
    parser.add_argument("--server-port", type=int, default=None)
    parser.add_argument("--multicast-group", default=MULTICAST_GROUP)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)
    args = parser.parse_args()

    host = args.host or get_local_ip()
    client_port = args.client_port or find_free_port()
    server_port = args.server_port or find_free_port()
    server_id = derive_server_id(host, server_port)

    ChatServer(
        server_id=server_id,
        host=host,
        client_port=client_port,
        server_port=server_port,
        multicast_group=args.multicast_group,
        discovery_port=args.discovery_port,
    ).start()


if __name__ == "__main__":
    main()
