# Distributed Live Chat System

A fault-tolerant chat: multiple servers elect a coordinator (Bully, highest ID wins).
Clients find the coordinator via UDP broadcast and reconnect automatically if it fails.

## Run servers

Start as many as you like (each in its own terminal). ID and ports are chosen automatically:

```bash
python server.py
```

The server with the highest ID becomes the coordinator, the rest are backups.

## Run clients

```bash
python client.py --username Alice
python client.py --username Bob --room general
```

Type messages directly; `/quit` to leave.

## Test failover

Stop the coordinator (`Ctrl+C`). After ~3 s of missing heartbeats the backups run a
Bully election; the highest-ID server takes over. Clients auto-reconnect and resend
any unacknowledged messages (no loss, no duplicates).

## How it works

- **Discovery:** clients → UDP broadcast (port 5973); servers → UDP multicast (224.1.1.1)
- **Chat:** TCP between client and coordinator (ordered via a global sequence number)
- **Fault tolerance:** heartbeats (1 s) + failure detection, full state sync to backups,
  client-side outbox with `msg_id` deduplication

## Files

- `server.py` – server (coordinator / backup, election, replication)
- `client.py` – chat client (discovery, auto-reconnect, outbox)
- `protocol.py` – shared message format and helpers
