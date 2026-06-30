import argparse
import queue
import socket
import threading
import time
from typing import Optional, Dict

from protocol import *

MULTICAST_GROUP = "224.1.1.1"
DISCOVERY_PORT = 5973
RECONNECT_DELAY = 2.0


def discover_coordinator(
    multicast_group: str,
    discovery_port: int,
    timeout: float = 2.0
) -> Optional[Dict]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
    sock.settimeout(timeout)

    try:
        request = make_message(DISCOVERY_REQUEST, client_probe=True)
        sock.sendto(encode(request), (multicast_group, discovery_port))

        deadline = time.time() + timeout
        best = None

        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(BUFFER_SIZE)
                response = decode(data)

                if response.get("type") != DISCOVERY_RESPONSE:
                    continue

                payload = response.get("payload", {})
                coordinator = payload.get("coordinator")

                if coordinator:
                    best = coordinator
                    break

            except socket.timeout:
                break

        return best

    finally:
        sock.close()


def receive_loop(
    sock: socket.socket,
    reconnect_event: threading.Event,
    redirect_holder: Dict,
    is_rejoin: bool,
    last_sequence: Dict,
) -> None:
    try:
        for message in read_json_lines(sock):
            msg_type = message.get("type")
            payload = message.get("payload", {})

            if msg_type == JOIN_ACCEPTED:
                if not is_rejoin:
                    print(f"Joined room '{payload['room']}' as {payload['client_id']}")
                    print("Participants:", ", ".join(payload.get("participants", [])))
                    for item in payload.get("recent_messages", []):
                        print(f"#{item['sequence']} {item['sender_name']}: {item['text']}")
                        last_sequence["seq"] = item["sequence"]

            elif msg_type == ORDERED_MESSAGE:
                item = payload["message"]
                seq = item["sequence"]
                if seq > last_sequence["seq"]:
                    print(f"#{seq} {item['sender_name']}: {item['text']}")
                    last_sequence["seq"] = seq

            elif msg_type == REDIRECT:
                coord = payload.get("coordinator")
                if coord:
                    redirect_holder["coordinator"] = coord

            elif msg_type == ERROR:
                print("Error:", payload.get("reason"))

    except Exception:
        pass
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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chat client using multicast discovery or direct TCP connection"
    )

    parser.add_argument("--username", required=True)
    parser.add_argument("--room", default="general")
    parser.add_argument("--client-id", default=None)

    parser.add_argument("--multicast-group", default=MULTICAST_GROUP)
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
        print(f"Using direct connection: {args.host}:{args.port}")
    else:
        static_coordinator = None

    input_queue: queue.Queue = queue.Queue()
    threading.Thread(target=input_reader, args=(input_queue,), daemon=True).start()

    coordinator = static_coordinator or discover_coordinator(args.multicast_group, args.discovery_port)
    if not coordinator:
        raise SystemExit("No coordinator found. Start at least one server first.")

    is_rejoin = False
    last_sequence: Dict = {"seq": 0}

    while True:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((coordinator["host"], int(coordinator["client_port"])))

            send_json_tcp(sock, make_message(
                JOIN_REQUEST,
                client_id=client_id,
                username=username,
                room=room,
            ))

            reconnect_event = threading.Event()
            redirect_holder: Dict = {}

            threading.Thread(
                target=receive_loop,
                args=(sock, reconnect_event, redirect_holder, is_rejoin, last_sequence),
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
                    send_json_tcp(sock, make_message(CHAT_MESSAGE, text=text))
                except Exception:
                    input_queue.put(text)
                    reconnect_event.set()

        except (ConnectionRefusedError, TimeoutError, OSError):
            pass
        finally:
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
            found = discover_coordinator(args.multicast_group, args.discovery_port)
            if found:
                coordinator = found


if __name__ == "__main__":
    main()
