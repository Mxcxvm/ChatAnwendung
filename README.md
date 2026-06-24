# Distributed Live Chat System Clear Specification Prototype

This prototype turns the abstract phrases from the project form into concrete components and message flows.

## Components

- `Client`
  - sends a multicast `DISCOVERY_REQUEST`
  - receives a `DISCOVERY_RESPONSE` containing the current coordinator address
  - opens a TCP connection to the coordinator
  - sends `JOIN_REQUEST`
  - sends `CHAT_MESSAGE` after joining

- `CoordinatorServer`
  - answers discovery requests with its own coordinator address
  - registers clients
  - assigns clients to rooms
  - orders messages with a global sequence number
  - distributes ordered messages to clients
  - synchronizes state to backup servers
  - sends heartbeat messages every second

- `BackupServer`
  - answers discovery requests with a redirect to the known coordinator
  - stores synchronized copies of client, room, and message state
  - monitors coordinator heartbeat messages
  - starts Bully election after missed heartbeats

- `ServerRegistry`
  - represented by the `servers` dictionary inside each server process
  - stores server ID, host, ports, and role

- `ClientRegistry`
  - represented by the `clients` dictionary inside the coordinator
  - stores client ID, username, room, and connection information

## Network communication

- Multicast UDP group `224.1.1.1:5973` for discovery and server announcements
- UDP server ports for heartbeat, state sync, and election messages
- TCP client ports for client join and chat messages

## Run three servers

Open three terminal windows.

```bash
python server.py --id 1 --client-port 10001 --server-port 11001
python server.py --id 2 --client-port 10002 --server-port 11002
python server.py --id 3 --client-port 10003 --server-port 11003
```

The server with the highest ID becomes coordinator.

## Run clients

Open two more terminal windows.

```bash
python client.py --username Alice --room general
python client.py --username Bob --room general
```

Write messages directly into the client terminal. Use `/quit` to leave.

## Test failover

Stop the current coordinator with `Ctrl+C`. Backup servers detect the missing heartbeat after about 3 seconds and start Bully election. The available server with the highest ID becomes the new coordinator.

## Important implementation note

A client does not discover the full server landscape. It discovers only a reachable entry point and receives the current coordinator address. The full server landscape is the set of active servers and is mainly used internally by the servers for coordination, state synchronization, heartbeat monitoring, and leader election.
