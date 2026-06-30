import argparse
import queue
import socket
import threading
import time
from typing import Optional, Dict, Any

from protocol import *

BROADCAST_ADDRESS = "255.255.255.255"
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


def discover_coordinator(
    broadcast_address: str,
    discovery_port: int,
    timeout: float = DISCOVERY_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(timeout)

        request = make_message(DISCOVERY_REQUEST, client_probe=True)

        sock.sendto(
            encode(request),
            (broadcast_address, discovery_port),
        )

        deadline = time.time() + timeout
        candidates = []

        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(BUFFER_SIZE)
                response = decode(data)

                if response.get("type") != DISCOVERY_RESPONSE:
                    continue

                payload = response.get("payload", {})
                coordinator = payload.get("coordinator")

                if valid_coordinator(coordinator):
                    candidates.append(coordinator)

            except socket.timeout:
                break
            except Exception:
                continue

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
        sock.close()


def receive_loop(
    sock: socket.socket,
    reconnect_event: threading.Event,
    redirect_holder: Dict[str, Any],
    is_rejoin: bool,
    last_sequence: Dict[str, int],
) -> None:
    buffer = b""
    last_received = time.time()

    try:
        sock.settimeout(RECV_TIMEOUT)

        while not reconnect_event.is_set():
            try:
                chunk = sock.recv(4096)

                if not chunk:
                    print("Connection closed by server.", flush=True)
                    break

                last_received = time.time()
                buffer += chunk

                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)

                    if not line.strip():
                        continue

                    message = decode(line)
                    msg_type = message.get("type")
                    payload = message.get("payload", {})

                    if msg_type == HEARTBEAT:
                        continue

                    elif msg_type == JOIN_ACCEPTED:
                        if not is_rejoin:
                            print(
                                f"Joined room '{payload['room']}' as {payload['client_id']}",
                                flush=True,
                            )
                            print(
                                "Participants:",
                                ", ".join(payload.get("participants", [])),
                                flush=True,
                            )
                        else:
                            print("Reconnected to coordinator.", flush=True)

                        for item in payload.get("recent_messages", []):
                            seq = int(item["sequence"])

                            if seq > last_sequence["seq"]:
                                print(
                                    f"#{seq} {item['sender_name']}: {item['text']}",
                                    flush=True,
                                )
                                last_sequence["seq"] = seq

                    elif msg_type == ORDERED_MESSAGE:
                        item = payload["message"]
                        seq = int(item["sequence"])

                        if seq > last_sequence["seq"]:
                            print(
                                f"#{seq} {item['sender_name']}: {item['text']}",
                                flush=True,
                            )
                            last_sequence["seq"] = seq

                    elif msg_type == REDIRECT:
                        coord = payload.get("coordinator")

                        if valid_coordinator(coord):
                            redirect_holder["coordinator"] = coord
                            print(
                                f"Redirected to coordinator "
                                f"{coord.get('host')}:{coord.get('client_port')}",
                                flush=True,
                            )
                            return

                    elif msg_type == JOIN_REJECTED:
                        print(
                            "Join rejected:",
                            payload.get("reason", "no reason given"),
                            flush=True,
                        )
                        return

                    elif msg_type == ERROR:
                        print("Error:", payload.get("reason"), flush=True)

            except socket.timeout:
                if time.time() - last_received > SERVER_SILENCE_TIMEOUT:
                    print("Server heartbeat timed out. Reconnecting...", flush=True)
                    break

            except OSError as exc:
                print(f"Connection lost: {exc}", flush=True)
                break

    except Exception as exc:
        print(f"Receive error: {exc}", flush=True)

    finally:
        reconnect_event.set()


def input_reader(input_queue: queue.Queue) -> None:
    while True:
        try:
            text = input()
            input_queue.put(text)
        except EOFError:
            input_queue.put(None)
            break


def connect_to_coordinator(coordinator: Dict[str, Any]) -> socket.socket:
    host = coordinator["host"]
    port = int(coordinator["client_port"])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(CONNECT_TIMEOUT)

    print(f"Connecting to {host}:{port} ...", flush=True)
    sock.connect((host, port))

    sock.settimeout(None)
    return sock


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chat client using UDP broadcast discovery or direct TCP connection"
    )

    parser.add_argument("--username", required=True)
    parser.add_argument("--room", default="general")
    parser.add_argument("--client-id", default=None)

    parser.add_argument("--broadcast-address", default=BROADCAST_ADDRESS)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)

    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)

    args = parser.parse_args()

    if (args.host is None) != (args.port is None):
        parser.error("Bitte entweder --host und --port zusammen angeben oder beide weglassen.")

    username = args.username
    room = args.room
    client_id = args.client_id or new_id("client")

    if args.host and args.port:
        static_coordinator = {
            "server_id": "manual",
            "host": args.host,
            "client_port": args.port,
        }
        print(f"Using direct connection: {args.host}:{args.port}", flush=True)
    else:
        static_coordinator = None

    input_queue: queue.Queue = queue.Queue()

    threading.Thread(
        target=input_reader,
        args=(input_queue,),
        daemon=True,
    ).start()

    if static_coordinator:
        coordinator = static_coordinator
    else:
        print("Searching coordinator via UDP broadcast...", flush=True)

        coordinator = discover_coordinator(
            args.broadcast_address,
            args.discovery_port,
        )

    if not coordinator:
        raise SystemExit("No coordinator found. Start at least one server first.")

    print(
        f"Coordinator found: {coordinator['host']}:{coordinator['client_port']}",
        flush=True,
    )

    is_rejoin = False
    last_sequence: Dict[str, int] = {"seq": 0}

    while True:
        redirect_holder: Dict[str, Any] = {}
        reconnect_event = threading.Event()
        sock: Optional[socket.socket] = None

        try:
            sock = connect_to_coordinator(coordinator)

            send_json_tcp(sock, make_message(
                JOIN_REQUEST,
                client_id=client_id,
                username=username,
                room=room,
            ))

            threading.Thread(
                target=receive_loop,
                args=(
                    sock,
                    reconnect_event,
                    redirect_holder,
                    is_rejoin,
                    last_sequence,
                ),
                daemon=True,
            ).start()

            is_rejoin = True

            while not reconnect_event.is_set():
                try:
                    text = input_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if text is None:
                    try:
                        send_json_tcp(sock, make_message(LEAVE))
                    except Exception:
                        pass
                    return

                if text.strip().lower() in {"/quit", "/exit"}:
                    try:
                        send_json_tcp(sock, make_message(LEAVE))
                    except Exception:
                        pass
                    return

                try:
                    send_json_tcp(sock, make_message(
                        CHAT_MESSAGE,
                        text=text,
                    ))
                except Exception as exc:
                    print(f"Send failed. Reconnecting: {exc}", flush=True)
                    input_queue.put(text)
                    reconnect_event.set()

        except KeyboardInterrupt:
            print("\nClient stopped.", flush=True)
            return

        except (ConnectionRefusedError, TimeoutError, OSError) as exc:
            print(f"Connection failed: {exc}", flush=True)

        finally:
            if sock is not None:
                try:
                    sock.close()
                except Exception:
                    pass

        time.sleep(RECONNECT_DELAY)

        next_coordinator = redirect_holder.get("coordinator")

        if next_coordinator:
            coordinator = next_coordinator

        elif static_coordinator:
            coordinator = static_coordinator

        else:
            print("Searching coordinator via UDP broadcast...", flush=True)

            found = discover_coordinator(
                args.broadcast_address,
                args.discovery_port,
            )

            if found:
                coordinator = found
                print(
                    f"Coordinator found: {coordinator['host']}:{coordinator['client_port']}",
                    flush=True,
                )
            else:
                print("No coordinator found yet. Retrying...", flush=True)


if __name__ == "__main__":
    main()