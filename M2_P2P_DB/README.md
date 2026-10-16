# M2_P2P_DB

A P2P distributed data store built on **iroh** (QUIC, NAT traversal, no central server) and **Automerge** (CRDT sync). Runs natively on Windows as a Rust executable — no Docker, no database server.

## Data model

Each record has exactly these fields (stored under `record:<uuid>` in the Automerge document):

```json
{
  "record_id": "1eea15db-86e4-43b9-b732-84eb222ff659",
  "updated_at": "2026-10-15T02:28:30+00:00",
  "ghname": "oahsiao",
  "loginname": "v-oahsiao",
  "cp_aic": 87226,
  "device_id": "local",
  "device_name": "MY-PC",
  "info1": "xx",
  "info2": "yy"
}
```

`record_id` is a fresh UUID at creation; `updated_at` is an ISO 8601 timestamp stamped on every write. `ghname` is the display name, `loginname` the account name. `device_id` marks the owning node's endpoint ID (defaults to `"local"`); `device_name` is a human-readable device name. The local UTC offset defaults to `+00:00`; set `M2_TZ_OFFSET_MINUTES=480` (for UTC+8) on the node's environment to emit local-offset timestamps.

> Schema note: the earlier `username` field was replaced by `ghname` + `loginname`, and `device_name` was added. Existing `doc.automerge` files written by older builds keep the old field names — new records read them as empty strings. Delete the data dir to start fresh.

## Build

From the repository root:

```bash
cargo build -p M2_P2P_DB
```

## Usage

Terminal 1 — start node A:

```bash
cargo run -p M2_P2P_DB -- listen
```

It prints its endpoint ID, e.g. `node endpoint id: 5105f536...`, and enables mDNS discovery.

Terminal 2 — start node B (same machine: use a different `--data-dir`; another machine: nothing else needed):

```bash
cargo run -p M2_P2P_DB -- --data-dir nodeB listen
```

Terminal 3 — CRUD:

```bash
cargo run -p M2_P2P_DB -- --data-dir nodeA create --ghname oahsiao --loginname v-oahsiao --cp-aic 87226 --info1 xx --info2 yy --device-name MY-PC
cargo run -p M2_P2P_DB -- --data-dir nodeA read <record-id>
cargo run -p M2_P2P_DB -- --data-dir nodeA update <record-id> --cp-aic 90000 --device-name NEW-PC
cargo run -p M2_P2P_DB -- --data-dir nodeA delete <record-id>
cargo run -p M2_P2P_DB -- --data-dir nodeA list
```

## Automatic node discovery and convergence

No manual connect is needed on a LAN:

1. **mDNS discovery** — every `listen` node announces itself on the local network (via `iroh-mdns-address-lookup`) and automatically learns about LAN peers. Discovered peers are recorded in the document.
2. **Peer list propagates with the data** — peers are stored under `peer:<endpoint-id>` keys inside the Automerge document, so the list syncs together with the records themselves.
3. **Gossip loop** — every 30 seconds each node syncs with every peer it knows, delta-only. Peers are synced **concurrently** (up to 32 at a time) with a 30-second per-peer timeout, so one unreachable node cannot delay or starve the rest.
4. **Peer expiry** — a peer that has not synced successfully for **3 days** is dropped from the peer list, and the removal propagates to the swarm. This keeps the list from accumulating dead nodes. Expiry is skipped for any round in which *no* peer was reachable: that state is indistinguishable from our own network being down, and letting a node that was offline for 3 days purge the shared list would delete the healthy swarm from the document — a deletion it could not itself recover from.

A new node on the LAN therefore converges with the whole swarm with zero manual steps. Across networks (no LAN), pass a seed peer once: `listen <peer-id>` or `connect <peer-id>` — after that the peer list propagates through the data and gossip takes over.

## Design notes

- **Records live directly under the Automerge `ROOT`, keyed by `record:<uuid>`.** A shared wrapper map (e.g. `records.0`) is an Automerge anti-pattern: two peers concurrently creating the wrapper map lose one side's entire subtree to last-writer-wins. Flat keys only conflict when the *same* record's fields are written concurrently, which is the desired LWW behavior.
- The document is persisted to `doc.automerge` (atomic rename). Because multiple processes (the `listen` node and one-shot CLI invocations) share a data dir, each sync refreshes by merging the on-disk document before sending and before persisting; a saved-length guard skips re-merging our own saves (Automerge rejects duplicate ops from the same actor).
- Sync state (`automerge::sync::State`) is kept per remote peer and reused across connections, so subsequent syncs send only deltas.
- The gossip loop never closes the shared endpoint; only one-shot CLI commands (`connect`) open and close their own endpoint.
- mDNS failure (restricted firewalls/VPNs) is non-fatal: the node logs a warning and falls back to manual `connect`/`listen <peer-id>` seeding.
- **The endpoint secret key is persisted** to `secret.key` (32 raw bytes) in the data dir, and is loaded on every `listen` start. This keeps the node's endpoint ID stable across restarts. Without it, each restart minted a fresh ID while the old one stayed in every peer's `peer:` list forever, so the swarm kept dialing dead IDs. Keep `secret.key` private — it *is* the node's identity. Delete it to deliberately start over with a new ID. (`connect` is a one-shot client and uses a throwaway key, so the peer it syncs with sees a fresh ID each time; that is fine, since `connect` never records itself as a peer.)
- **Peer last-seen times live in `peers.lastseen.json`, not in the document.** They change every gossip round, and every document write is a permanent Automerge op, so storing them there would grow the document without bound. The file is local to each node; only the *removal* of an expired peer is a document change, which propagates normally. A peer with no last-seen entry is given one the moment we learn about it (from mDNS, a seed, or the synced document), so a node we never reach still expires 3 days after we first heard of it rather than lingering forever.
- Concurrent syncs serialise their saves: `persist_snapshot`'s read-merge-write runs under a dedicated lock, and each save writes a uniquely named temp file before the atomic rename. Without both, two syncs finishing at once could clobber each other's save.
- Each round clears the peer's `sync::State::in_flight` **before** dialling. Automerge sets that flag when generating a message and clears it only when a reply arrives; any exchange that ends another way (peer closed first, I/O error, our own timeout) leaves it set, and `generate_sync_message` then returns `None` forever — the peer goes silently mute. Clearing it at the start of a round rather than after a failure matters because a timeout *drops the sync future mid-await*, so any cleanup written after that `.await` never runs.
