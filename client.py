import argparse
import socket
import threading
import time
from typing import Optional, Dict

from protocol import *

MULTICAST_GROUP = "224.1.1.1"
DISCOVERY_PORT = 5973


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


def receive_loop(sock: socket.socket) -> None:
    try:
        for message in read_json_lines(sock):
            msg_type = message.get("type")
            payload = message.get("payload", {})

            if msg_type == JOIN_ACCEPTED:
                print(f"Joined room '{payload['room']}' as {payload['client_id']}")
                print("Participants:", ", ".join(payload.get("participants", [])))

                for item in payload.get("recent_messages", []):
                    print(f"#{item['sequence']} {item['sender_name']}: {item['text']}")

            elif msg_type == ORDERED_MESSAGE:
                item = payload["message"]
                print(f"#{item['sequence']} {item['sender_name']}: {item['text']}")

            elif msg_type == REDIRECT:
                print("Redirected to coordinator:", payload.get("coordinator"))

            elif msg_type == ERROR:
                print("Error:", payload.get("reason"))

    except Exception as exc:
        print("receive loop stopped:", exc)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chat client using multicast discovery or direct TCP connection"
    )

    parser.add_argument("--username", required=True)
    parser.add_argument("--room", default="general")
    parser.add_argument("--client-id", default=None)

    parser.add_argument("--multicast-group", default=MULTICAST_GROUP)
    parser.add_argument("--discovery-port", type=int, default=DISCOVERY_PORT)

    # Neu: Direktverbindung über VPN/IP
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)

    args = parser.parse_args()

    if (args.host is None) != (args.port is None):
        parser.error("Bitte entweder --host und --port zusammen angeben oder beide weglassen.")

    if args.host and args.port:
        coordinator = {
            "server_id": "manual",
            "host": args.host,
            "client_port": args.port,
        }
        print(f"Using direct connection: {args.host}:{args.port}")
    else:
        coordinator = discover_coordinator(args.multicast_group, args.discovery_port)

        if not coordinator:
            raise SystemExit("No coordinator found. Start at least one server first.")

        print(
            f"Coordinator discovered: server {coordinator['server_id']} "
            f"at {coordinator['host']}:{coordinator['client_port']}"
        )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        sock.connect((coordinator["host"], int(coordinator["client_port"])))

        send_json_tcp(sock, make_message(
            JOIN_REQUEST,
            client_id=args.client_id or new_id("client"),
            username=args.username,
            room=args.room,
        ))

        threading.Thread(target=receive_loop, args=(sock,), daemon=True).start()

        while True:
            text = input()

            if text.strip().lower() in {"/quit", "/exit"}:
                send_json_tcp(sock, make_message(LEAVE))
                break

            send_json_tcp(sock, make_message(CHAT_MESSAGE, text=text))

    except ConnectionRefusedError:
        print("Connection refused. Der Server läuft nicht oder der Port ist falsch.")

    except TimeoutError:
        print("Connection timeout. IP, VPN oder Firewall prüfen.")

    except OSError as exc:
        print(f"Connection error: {exc}")

    finally:
        sock.close()


if __name__ == "__main__":
    main()