//! M2_P2P_DB — a P2P distributed data store built on iroh + Automerge.
//!
//! Each node keeps an Automerge CRDT document holding records with:
//!
//! ```json
//! {
//!   "record_id": "UUID",
//!   "updated_at": "2026-10-14T15:00:00+08:00",
//!   "ghname": "oahsiao",
//!   "loginname": "v-oahsiao",
//!   "cp_aic": 87226,
//!   "device_id": "Iroh Node ID",
//!   "device_name": "xx",
//!   "info1": "xx",
//!   "info2": "xx"
//! }
//! ```
//!
//! Records live under keys `record:<uuid>`; peers are tracked under
//! `peer:<endpoint-id>` and propagate with the data itself. Nodes discover
//! LAN peers via mDNS (no manual connect needed), and gossip-sync every 30
//! seconds, so the swarm aligns automatically. No central server, no Docker.
//!
//! Usage:
//!
//!     M2_P2P_DB listen
//!     M2_P2P_DB connect <endpoint-id>
//!     M2_P2P_DB create --ghname oahsiao --loginname v-oahsiao --cp-aic 87226 --info1 xx --info2 xx
//!     M2_P2P_DB read <record-id>
//!     M2_P2P_DB update <record-id> --cp-aic 90000
//!     M2_P2P_DB delete <record-id>
//!     M2_P2P_DB list

use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use automerge::{
    AutoCommit, ObjType, ReadDoc, ROOT,
    sync::{self, SyncDoc},
    transaction::Transactable,
};
use clap::{Parser, Subcommand};
use iroh::{
    Endpoint, EndpointId, RelayMode, SecretKey,
    address_lookup::{PkarrPublisher, PkarrResolver},
    endpoint::{Connection, presets},
    endpoint::presets::Preset,
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_mdns_address_lookup::{DiscoveryEvent, MdnsAddressLookup};
use n0_error::{Result, StdResultExt, anyerr};
use n0_future::StreamExt;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// ALPN identifying our sync protocol.
const ALPN: &[u8] = b"m2-p2p-db/sync/0";

/// Key prefix for peer-list entries stored in the document.
const PEER_PREFIX: &str = "peer:";

/// Key prefix for records stored in the document (`record:<uuid>`).
const RECORD_PREFIX: &str = "record:";

/// File inside the data dir holding this node's persisted endpoint secret key
/// (32 raw bytes). Persisting it keeps the endpoint ID stable across restarts
/// — otherwise every restart mints a new ID, stranding the old one as a
/// zombie `peer:` entry that every peer keeps dialing forever.
const KEY_FILE: &str = "secret.key";

/// Local (non-propagated) file recording the unix-ms of our last successful
/// sync with each peer, so the peer list can drop nodes that have gone away
/// for good. Deliberately *not* stored in the CRDT: it changes every round,
/// and every write to the document is a permanent op, so a per-round
/// timestamp there would grow the document without bound.
const PEER_SEEN_FILE: &str = "peers.lastseen.json";

/// How often the gossip loop syncs with every known peer.
const GOSSIP_INTERVAL: Duration = Duration::from_secs(30);

/// Upper bound on one peer's sync (connect + exchange). An unreachable peer is
/// abandoned for this round rather than stalling it; the next round retries
/// with the peer's retained sync state, so the retry is a cheap delta.
const SYNC_TIMEOUT: Duration = Duration::from_secs(30);

/// Cap on concurrent per-peer syncs, so a long peer list cannot open hundreds
/// of connections at once.
const MAX_CONCURRENT_SYNCS: usize = 32;

/// A peer we have not synced with successfully for this long is dropped from
/// the peer list (and the removal propagates to the rest of the swarm).
///
/// Was 3 days; dropped to 1 day because each zombie stays on the dial list
/// for the full TTL, and a day of accumulated zombies is enough to clog
/// the gossip round without delaying actual data exchange any further.
const PEER_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Cap on the peer list size. When the peer list grows past this, the
/// entries with the oldest last-seen are evicted (and removed from the
/// document) — protects against a swarm that learns dozens of zombies in
/// a single document-sync round, all of which would otherwise stay on the
/// dial list until TTL.
const MAX_PEERS: usize = 64;

/// If we have never reached a peer after this many gossip rounds, drop it.
/// This is the proactive half of zombie cleanup: TTL is reactive (waits 1
/// day), but a peer whose ID keeps coming in via document sync can pile up
/// faster than that, and the only signal we have is "how many rounds have
/// we tried it". 8 rounds at the default 30 s interval ≈ 4 minutes.
const ZOMBIE_FAIL_ROUNDS: u32 = 8;

/// Whether a gossip round's outcome permits expiring peers.
///
/// Expiry deletes entries from the *shared* document, and that deletion
/// propagates. A round where every peer was unreachable is indistinguishable
/// from our own network being down, so expiring then would let a node that was
/// offline for longer than [`PEER_TTL`] delete the whole — healthy — swarm
/// from the document, with no way to recover the list by itself. Only expire
/// when we actually reached someone, or when there was nobody to try.
fn may_expire_peers(attempted: usize, any_reached: bool) -> bool {
    attempted == 0 || any_reached
}

/// (Records live directly under the automerge ROOT, keyed by
/// `record:<uuid>`. A shared wrapper map is an Automerge anti-pattern:
/// two peers concurrently creating the wrapper map would lose one side's
/// entire subtree to LWW. Flat keys only conflict when the *same* record is
/// written concurrently, which merges as LWW per field.)

#[derive(Debug, Parser)]
#[command(name = "M2_P2P_DB", about = "P2P distributed data store (iroh + automerge)")]
struct Cli {
    /// Directory where the automerge document is persisted.
    #[arg(long, default_value = "m2-p2p-db-data")]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start a node: mDNS discovery, gossip sync, accept connections.
    Listen {
        /// Endpoint IDs of peers to sync with on startup (optional).
        peers: Vec<EndpointId>,
    },
    /// Sync once with a peer, then exit.
    Connect {
        /// Endpoint ID of the peer.
        endpoint_id: EndpointId,
    },
    /// Create a new record (record_id is a fresh UUID).
    Create {
        /// GitHub-style display name.
        #[arg(long)]
        ghname: String,
        /// Login/account name.
        #[arg(long)]
        loginname: String,
        #[arg(long)]
        cp_aic: f64,
        #[arg(long, default_value = "")]
        info1: String,
        #[arg(long, default_value = "")]
        info2: String,
        /// Endpoint ID to stamp as device_id (defaults to "local" unless given).
        #[arg(long)]
        device_id: Option<String>,
        /// Human-readable device name.
        #[arg(long, default_value = "")]
        device_name: String,
    },
    /// Read a record by UUID.
    Read { record_id: Uuid },
    /// Update fields of an existing record.
    Update {
        record_id: Uuid,
        #[arg(long)]
        ghname: Option<String>,
        #[arg(long)]
        loginname: Option<String>,
        #[arg(long)]
        cp_aic: Option<f64>,
        #[arg(long)]
        info1: Option<String>,
        #[arg(long)]
        info2: Option<String>,
        #[arg(long)]
        device_name: Option<String>,
    },
    /// Delete a record.
    Delete { record_id: Uuid },
    /// List all records as JSON.
    List,
    /// Drop zombie peers: anything in the document under `peer:` that has
    /// either no last-seen entry or whose fail-round counter is at the cap.
    /// Run with the node offline (no listener) so it doesn't fight an
    /// in-flight gossip round. Safe to run repeatedly.
    Prune,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()))
        .init();

    let cli = Cli::parse();
    let store = Store::open(&cli.data_dir).await?;

    match cli.command {
        Command::Listen { peers } => run_listen(store, peers).await,
        Command::Connect { endpoint_id } => {
            // Minimal.apply sets the rustls crypto provider; no
            // address-lookups needed for a one-shot connect.
            let endpoint = presets::Minimal.apply(Endpoint::builder(presets::Empty)).bind().await?;
            endpoint.online().await;
            store.sync_once(&endpoint, endpoint_id).await?;
            endpoint.close().await;
            println!("sync with {endpoint_id} complete");
            Ok(())
        }
        Command::Create { ghname, loginname, cp_aic, info1, info2, device_id, device_name } => {
            let record_id = Uuid::new_v4().to_string();
            let device_id = device_id.unwrap_or_else(|| "local".to_string());
            store
                .upsert_record(&record_id, &RecordUpsert {
                    ghname, loginname, cp_aic, info1, info2, device_id, device_name,
                })
                .await?;
            println!("created {record_id}");
            print_record(&store, &record_id.to_string()).await;
            Ok(())
        }
        Command::Read { record_id } => {
            if store.read_record(&record_id.to_string()).await?.is_none() {
                eprintln!("record '{record_id}' not found");
                std::process::exit(1);
            }
            print_record(&store, &record_id.to_string()).await;
            Ok(())
        }
        Command::Update { record_id, ghname, loginname, cp_aic, info1, info2, device_name } => {
            let Some(current) = store.read_record(&record_id.to_string()).await? else {
                eprintln!("record '{record_id}' not found");
                std::process::exit(1);
            };
            let upsert = RecordUpsert {
                ghname: ghname.unwrap_or(current.ghname),
                loginname: loginname.unwrap_or(current.loginname),
                cp_aic: cp_aic.unwrap_or(current.cp_aic),
                info1: info1.unwrap_or(current.info1),
                info2: info2.unwrap_or(current.info2),
                device_id: current.device_id,
                device_name: device_name.unwrap_or(current.device_name),
            };
            store.upsert_record(&record_id.to_string(), &upsert).await?;
            println!("updated {record_id}");
            print_record(&store, &record_id.to_string()).await;
            Ok(())
        }
        Command::Delete { record_id } => {
            store.delete_record(&record_id.to_string()).await?;
            println!("deleted {record_id}");
            Ok(())
        }
        Command::List => {
            for rec in store.list_records().await? {
                println!("{rec}");
            }
            Ok(())
        }
        Command::Prune => {
            // Single-shot offline cleanup. Drop every `peer:` key whose
            // local last-seen row says zombie (fail-round cap reached, TTL
            // expired, or no row at all = we never observed it).
            let now = Store::now_ms();
            let cutoff = now.saturating_sub(PEER_TTL.as_millis() as u64);
            let stale: Vec<EndpointId> = {
                let seen = store.peer_seen.lock().await;
                let doc_peers: Vec<EndpointId> = {
                    let inner = store.inner.lock().await;
                    inner
                        .doc
                        .keys(ROOT)
                        .filter_map(|k| {
                            let s = k.to_string();
                            s.strip_prefix(PEER_PREFIX)
                                .and_then(|id| EndpointId::from_str(id).ok())
                        })
                        .collect()
                };
                doc_peers
                    .into_iter()
                    .filter(|id| match seen.get(id) {
                        None => true, // never observed → drop
                        Some(p) => {
                            p.last_seen_ms != 0 && p.last_seen_ms < cutoff
                                || p.fail_rounds >= ZOMBIE_FAIL_ROUNDS
                        }
                    })
                    .collect()
            };
            if stale.is_empty() {
                println!("no zombie peers to drop");
            } else {
                println!("dropping {} zombie peer(s) from the document:", stale.len());
                for id in &stale {
                    println!("  - {id}");
                }
                if let Err(e) = Store::remove_peers_pub(&store, &stale).await {
                    eprintln!("prune failed: {e}");
                    std::process::exit(1);
                }
                println!("done");
            }
            Ok(())
        }
    }
}

/// Load this node's endpoint secret key from `data_dir`, generating and
/// persisting one on first run.
///
/// Keeping the key stable across restarts keeps the endpoint ID stable, which
/// is what makes the peer list (and mDNS/`connect` seeds) survive a restart.
/// The file is created with restrictive permissions where the platform
/// supports it.
async fn load_or_create_secret_key(data_dir: &Path) -> Result<SecretKey> {
    let path = data_dir.join(KEY_FILE);
    if path.exists() {
        let bytes = tokio::fs::read(&path).await.anyerr()?;
        let arr: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
            anyerr!("secret key file {} is corrupt: expected 32 bytes, found {}; \
                     delete it to regenerate a fresh identity",
                    path.display(), bytes.len())
        })?;
        return Ok(SecretKey::from_bytes(&arr));
    }

    let key = SecretKey::generate();
    // Write to a temp file then rename, so a crash never leaves a partial key.
    let tmp = data_dir.join(format!("{KEY_FILE}.tmp"));
    tokio::fs::write(&tmp, key.to_bytes()).await.anyerr()?;
    tokio::fs::rename(&tmp, &path).await.anyerr()?;
    Ok(key)
}

async fn run_listen(store: Store, peers: Vec<EndpointId>) -> Result<()> {
    let secret_key = load_or_create_secret_key(&store.data_dir).await?;
    // Build the endpoint with only the HTTPS-based address-lookups
    // (PkarrPublisher + PkarrResolver). The n0 preset also wires in
    // `DnsAddressLookup::n0_dns()`, which queries DNS TXT records over UDP
    // and, when the response is truncated, opens a fresh TCP connection per
    // query via hickory-resolver. With peer churn we get a TCP-53 SYN/FIN
    // burst every few seconds; cutting DnsAddressLookup makes the DNS path
    // disappear from the steady state. Relay hostnames still resolve once
    // at startup via the default resolver, which is fine.
    // Minimal sets the rustls crypto provider; the rest is the same as
    // `N0` minus the DNS-TXT address lookup that was causing the TCP burst.
    let mut builder = presets::Minimal.apply(Endpoint::builder(presets::Empty));
    builder = builder
        .secret_key(secret_key)
        .relay_mode(RelayMode::Default)
        .address_lookup(PkarrPublisher::n0_dns())
        .address_lookup(PkarrResolver::n0_dns());
    let endpoint = builder.bind().await?;
    let router = Router::builder(endpoint.clone())
        .accept(ALPN, SyncHandler(store.clone()))
        .spawn();

    endpoint.online().await;
    let id = endpoint.id();
    println!("node endpoint id: {id}");
    println!("data dir: {}", store.data_dir.display());
    println!("listening for sync connections (ALPN {})", String::from_utf8_lossy(ALPN));

    // mDNS discovery: peers on the same LAN find each other automatically
    // and are recorded into the document, joining the gossip swarm with no
    // manual connect at all.
    let mdns = match MdnsAddressLookup::builder().build(id) {
        Ok(mdns) => {
            endpoint.address_lookup().anyerr()?.add(mdns.clone());
            println!("mdns discovery: enabled (LAN peers are discovered automatically)");
            Some(mdns)
        }
        Err(e) => {
            // Not fatal: mdns can fail where sockets are restricted (some
            // firewalls/VPNs). Gossip via manual connect still works.
            eprintln!("mdns discovery unavailable ({e}); falling back to manual connect");
            None
        }
    };

    // Record our own ID and any explicitly given peers in the document, so
    // the peer list propagates with the data itself.
    store.add_peers(&peers).await?;
    store.add_peers(std::slice::from_ref(&id)).await?;

    // Initial sync with the explicitly listed peers.
    for peer in &peers {
        let store = store.clone();
        let endpoint = endpoint.clone();
        let peer = *peer;
        tokio::spawn(async move {
            if let Err(e) = store.sync_once(&endpoint, peer).await {
                eprintln!("failed to sync with {peer}: {e}");
            }
        });
    }

    // mDNS event loop: every LAN peer discovered joins the peer list.
    if let Some(mdns) = mdns {
        let mdns_store = store.clone();
        tokio::spawn(async move {
            let mut events = mdns.subscribe().await;
            while let Some(event) = events.next().await {
                if let DiscoveryEvent::Discovered { endpoint_info, .. } = event {
                    if let Err(e) = mdns_store.add_peers(&[endpoint_info.endpoint_id]).await {
                        tracing::warn!("failed to record discovered peer: {e}");
                    }
                }
            }
        });
    }

    // Gossip loop: periodically sync with every peer known from the
    // document. Learned peers propagate automatically, so a new node only
    // needs to reach one existing node to converge with the whole swarm.
    let mut ticker = tokio::time::interval(GOSSIP_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let gossip_store = store.clone();
    let gossip_endpoint = endpoint.clone();
    let gossip = tokio::spawn(async move {
        // First tick fires immediately; skip it (initial syncs above).
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let known = match gossip_store.known_peers().await {
                Ok(peers) => peers,
                Err(e) => {
                    tracing::warn!("failed to read known peers: {e}");
                    continue;
                }
            };
            let self_id = gossip_endpoint.id();

            // Give any newly learned peer an expiry baseline before we dial it,
            // so a peer we never manage to reach still ages out after 3 days.
            let to_seed: Vec<EndpointId> =
                known.iter().copied().filter(|p| *p != self_id).collect();
            gossip_store.seed_peer_baselines(&to_seed).await;

            // Sync every peer concurrently, bounded by a semaphore so a long
            // peer list cannot open hundreds of connections at once. Each sync
            // has its own timeout: one unreachable peer consumes only its own
            // slot and is abandoned for this round, instead of stalling every
            // peer queued behind it (which is what starved the swarm before).
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_SYNCS));
            let mut tasks = tokio::task::JoinSet::new();
            for peer in known {
                if peer == self_id {
                    continue;
                }
                let store = gossip_store.clone();
                let endpoint = gossip_endpoint.clone();
                let sem = sem.clone();
                // Each task reports whether it reached the peer, so the round
                // can tell "the others went away" from "we are the ones who
                // are offline".
                tasks.spawn(async move {
                    let _permit = match sem.acquire_owned().await {
                        Ok(p) => p,
                        Err(_) => return None,
                    };
                    match tokio::time::timeout(SYNC_TIMEOUT, store.sync_once(&endpoint, peer)).await
                    {
                        Ok(Ok(())) => {
                            store.mark_peer_seen(peer).await;
                            Some(())
                        }
                        Ok(Err(e)) => {
                            tracing::debug!("gossip sync with {peer} failed: {e}");
                            // Bump the proactive zombie counter even on
                            // non-timeout failures: a peer that consistently
                            // errors (auth failed, wrong ALPN, etc.) wastes
                            // the same slot as a timed-out one.
                            store.bump_peer_fail(peer).await;
                            None
                        }
                        Err(_) => {
                            tracing::debug!(
                                "gossip sync with {peer} timed out after {}s",
                                SYNC_TIMEOUT.as_secs()
                            );
                            store.bump_peer_fail(peer).await;
                            None
                        }
                    }
                });
            }
            let mut any_reached = false;
            let mut attempted = 0usize;
            while let Some(joined) = tasks.join_next().await {
                attempted += 1;
                if matches!(joined, Ok(Some(()))) {
                    any_reached = true;
                }
            }

            // Persist what we learned, then expire peers that have gone for
            // good — but only if we actually reached someone this round. A
            // round in which *every* peer is unreachable is indistinguishable
            // from our own network being down, and expiry here is a permanent,
            // propagating deletion: a node that was offline for 3 days would
            // otherwise delete the entire — healthy — swarm from the shared
            // document, and could not recover the list on its own.
            gossip_store.save_peer_seen().await;
            if may_expire_peers(attempted, any_reached) {
                gossip_store.expire_stale_peers(self_id).await;
            } else {
                tracing::debug!(
                    "skipping peer expiry: no peer was reachable this round, \
                     so we may be the one that is offline"
                );
            }
        }
    });

    // Keep running until Ctrl+C.
    tokio::signal::ctrl_c().await.anyerr()?;
    gossip.abort();
    println!("shutting down");
    router.shutdown().await.anyerr()?;
    Ok(())
}

/// A record as stored in the automerge document.
#[derive(Debug, Clone, serde::Serialize)]
struct Record {
    record_id: String,
    /// ISO 8601 timestamp with local offset, e.g. `2026-10-14T15:00:00+08:00`.
    updated_at: String,
    /// GitHub-style display name.
    ghname: String,
    /// Login/account name.
    loginname: String,
    cp_aic: f64,
    /// Iroh node ID of the device that owns/last wrote this record.
    device_id: String,
    device_name: String,
    info1: String,
    info2: String,
}

/// The writable fields of a record (everything except `record_id` and
/// `updated_at`, which are managed: UUID at creation, timestamp on write).
#[derive(Debug, Clone)]
struct RecordUpsert {
    ghname: String,
    loginname: String,
    cp_aic: f64,
    device_id: String,
    device_name: String,
    info1: String,
    info2: String,
}

impl std::fmt::Display for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ \"record_id\": {:?}, \"updated_at\": {:?}, \"ghname\": {:?}, \"loginname\": {:?}, \
             \"cp_aic\": {}, \"device_id\": {:?}, \"device_name\": {:?}, \"info1\": {:?}, \"info2\": {:?} }}",
            self.record_id, self.updated_at, self.ghname, self.loginname,
            self.cp_aic, self.device_id, self.device_name, self.info1, self.info2
        )
    }
}

impl Store {
    /// Public wrapper around the private [`Store::remove_peers`] for the
    /// `Prune` subcommand. The method is already correct; it is just hidden
    /// from `pub` because nothing else in the listen path needs it.
    async fn remove_peers_pub(&self, peers: &[EndpointId]) -> Result<()> {
        self.remove_peers(peers).await
    }
}

/// Local document store: automerge doc + persistence + sync helpers.
#[derive(Debug, Clone)]
struct Store {
    inner: std::sync::Arc<Mutex<StoreInner>>,
    data_dir: PathBuf,
    /// Serialises the read-merge-write sequence of `persist_snapshot` and
    /// `remove_peers`. `inner` alone is not enough: the disk read in between
    /// has to be part of the same critical section, or two concurrent savers
    /// can each merge the same on-disk state and write it back.
    persist_lock: std::sync::Arc<Mutex<()>>,
    /// Distinguishes the temp file each on-disk write uses, so concurrent
    /// saves cannot overwrite each other's temp file (used by both the
    /// document and the last-seen table).
    tmp_seq: std::sync::Arc<AtomicU64>,
    /// When we last synced successfully with each peer (unix ms). Local only,
    /// used to expire peers that have left for good. See [`PEER_SEEN_FILE`].
    ///
    /// The `fail_rounds` counter is the proactive zombie cleanup companion:
    /// incremented every gossip round a peer fails to respond to, cleared
    /// on every successful sync. When it passes [`ZOMBIE_FAIL_ROUNDS`] the
    /// peer is dropped immediately (independent of [`PEER_TTL`]).
    peer_seen: std::sync::Arc<Mutex<std::collections::HashMap<EndpointId, PeerSeen>>>,
}

/// One row of [`Store::peer_seen`].
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, Default)]
struct PeerSeen {
    /// Unix ms of the last successful sync with this peer.
    last_seen_ms: u64,
    /// Consecutive gossip rounds this peer has failed to respond to.
    /// Reset to 0 on every successful sync.
    fail_rounds: u32,
}

#[derive(Debug)]
struct StoreInner {
    doc: AutoCommit,
    /// One sync state per remote peer we've synced with.
    sync_states: std::collections::HashMap<EndpointId, sync::State>,
    /// Length of the on-disk document at the time we last loaded/merged it.
    /// Used to skip re-merging our own saves (automerge rejects duplicate
    /// ops from the same actor).
    disk_len: Option<u64>,
}

impl Store {
    async fn open(data_dir: &PathBuf) -> Result<Self> {
        tokio::fs::create_dir_all(data_dir).await.anyerr()?;
        let doc_path = data_dir.join("doc.automerge");
        let (doc, disk_len) = if doc_path.exists() {
            let bytes = tokio::fs::read(&doc_path).await.anyerr()?;
            (AutoCommit::load(&bytes).anyerr()?, Some(bytes.len() as u64))
        } else {
            (AutoCommit::new(), None)
        };
        // Load local last-synced timestamps. A corrupt or missing file just
        // means "we don't know yet"; peers are then only dropped once we have
        // an observation to act on, so this never mass-deletes the peer list.
        let seen_path = data_dir.join(PEER_SEEN_FILE);
        // File shape changed: `last_seen_ms` (was `last_seen`) + new
        // `fail_rounds`. Tolerate either the new shape or the old
        // `{ "id": u64_ms }` so a pre-existing file still loads without
        // re-learning every peer.
        let peer_seen = match tokio::fs::read(&seen_path).await {
            Ok(bytes) => {
                // Try the new shape first.
                if let Ok(m) = serde_json::from_slice::<
                    std::collections::HashMap<String, PeerSeen>,
                >(&bytes)
                {
                    m.into_iter()
                        .filter_map(|(k, v)| {
                            EndpointId::from_str(&k).ok().map(|id| (id, v))
                        })
                        .collect()
                } else if let Ok(m) =
                    serde_json::from_slice::<std::collections::HashMap<String, u64>>(&bytes)
                {
                    // Old shape: every u64 is a unix-ms timestamp; assume
                    // a clean state for fail_rounds.
                    m.into_iter()
                        .filter_map(|(k, v)| {
                            EndpointId::from_str(&k).ok().map(|id| {
                                (id, PeerSeen { last_seen_ms: v, fail_rounds: 0 })
                            })
                        })
                        .collect()
                } else {
                    Default::default()
                }
            }
            Err(_) => Default::default(),
        };

        Ok(Self {
            inner: std::sync::Arc::new(Mutex::new(StoreInner {
                doc,
                sync_states: Default::default(),
                disk_len,
            })),
            data_dir: data_dir.clone(),
            persist_lock: std::sync::Arc::new(Mutex::new(())),
            tmp_seq: std::sync::Arc::new(AtomicU64::new(0)),
            peer_seen: std::sync::Arc::new(Mutex::new(peer_seen)),
        })
    }

    async fn persist(&self, doc: &mut AutoCommit) -> Result<()> {
        let path = self.data_dir.join("doc.automerge");
        let bytes = doc.save();
        // Write to a temp file then rename for basic crash safety. The temp
        // name is unique per call so concurrent saves cannot clobber each
        // other's temp file (the rename itself is atomic).
        let n = self.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = self.data_dir.join(format!("doc.automerge.{n}.tmp"));
        tokio::fs::write(&tmp, bytes).await.anyerr()?;
        tokio::fs::rename(&tmp, &path).await.anyerr()?;
        Ok(())
    }

    fn now_iso8601() -> String {
        // RFC 3339 / ISO 8601 with the local UTC offset, e.g.
        // `2026-10-14T15:00:00+08:00`.
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let (date, time, offset) = civil_from_unix(secs);
        format!("{date}T{time}{offset}")
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    async fn read_record(&self, record_id: &str) -> Result<Option<Record>> {
        let inner = self.inner.lock().await;
        Self::read_record_inner(&inner.doc, record_id)
    }

    fn read_record_inner(doc: &AutoCommit, record_id: &str) -> Result<Option<Record>> {
        let key = format!("{RECORD_PREFIX}{record_id}");
        let rec = match doc.get(ROOT, &key).anyerr()? {
            Some((automerge::Value::Object(ObjType::Map), obj)) => obj,
            _ => return Ok(None),
        };
        let get_str = |field: &str| -> Result<String> {
            Ok(doc
                .get(&rec, field)
                .anyerr()?
                .and_then(|(v, _)| v.as_str().map(str::to_string))
                .unwrap_or_default())
        };
        let cp_aic = match doc.get(&rec, "cp_aic").anyerr()? {
            Some((v, _)) => as_f64(&v)?,
            None => 0.0,
        };
        Ok(Some(Record {
            record_id: record_id.to_string(),
            updated_at: get_str("updated_at")?,
            ghname: get_str("ghname")?,
            loginname: get_str("loginname")?,
            cp_aic,
            device_id: get_str("device_id")?,
            device_name: get_str("device_name")?,
            info1: get_str("info1")?,
            info2: get_str("info2")?,
        }))
    }

    async fn upsert_record(&self, record_id: &str, fields: &RecordUpsert) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            let key = format!("{RECORD_PREFIX}{record_id}");
            let rec = match inner.doc.get(ROOT, &key).anyerr()? {
                Some((automerge::Value::Object(ObjType::Map), obj)) => obj,
                _ => inner.doc.put_object(ROOT, &key, ObjType::Map).anyerr()?,
            };
            inner
                .doc
                .put(&rec, "updated_at", Self::now_iso8601())
                .anyerr()?;
            inner.doc.put(&rec, "ghname", &fields.ghname).anyerr()?;
            inner.doc.put(&rec, "loginname", &fields.loginname).anyerr()?;
            inner.doc.put(&rec, "cp_aic", fields.cp_aic).anyerr()?;
            inner.doc.put(&rec, "device_id", &fields.device_id).anyerr()?;
            inner.doc.put(&rec, "device_name", &fields.device_name).anyerr()?;
            inner.doc.put(&rec, "info1", &fields.info1).anyerr()?;
            inner.doc.put(&rec, "info2", &fields.info2).anyerr()?;
        }
        self.persist_snapshot().await?;
        Ok(())
    }

    async fn delete_record(&self, record_id: &str) -> Result<()> {
        {
            let mut inner = self.inner.lock().await;
            let key = format!("{RECORD_PREFIX}{record_id}");
            if inner.doc.get(ROOT, &key).anyerr()?.is_some() {
                inner.doc.delete(ROOT, &key).anyerr()?;
            }
        }
        self.persist_snapshot().await?;
        Ok(())
    }

    async fn list_records(&self) -> Result<Vec<Record>> {
        let inner = self.inner.lock().await;
        let doc = &inner.doc;
        let mut out = Vec::new();
        for key in doc.keys(ROOT) {
            let key = key.to_string();
            // Skip peer-list entries (`peer:<id>`); they are metadata, not records.
            if key.starts_with(PEER_PREFIX) {
                continue;
            }
            if let Some(record_id) = key.strip_prefix(RECORD_PREFIX) {
                if let Some(rec) = Self::read_record_inner(doc, record_id)? {
                    out.push(rec);
                }
            }
        }
        out.sort_by(|a, b| a.record_id.cmp(&b.record_id));
        Ok(out)
    }

    /// Record peers in the document under `peer:<id>` keys so the peer list
    /// itself propagates with sync. The value is a timestamp (when we first
    /// saw them).
    async fn add_peers(&self, peers: &[EndpointId]) -> Result<()> {
        let mut changed = false;
        let mut newly_seeded = Vec::new();
        {
            let mut inner = self.inner.lock().await;
            for peer in peers {
                let key = format!("{PEER_PREFIX}{peer}");
                if inner.doc.get(ROOT, &key).anyerr()?.is_none() {
                    inner.doc.put(ROOT, &key, Self::now_ms()).anyerr()?;
                    changed = true;
                    newly_seeded.push(*peer);
                }
            }
        }
        // Give every peer a last-seen baseline the moment we learn about it.
        // Without this, a peer we have *never* reached (the usual zombie case:
        // an ID that a restart orphaned) would have no entry and so be exempt
        // from expiry forever — exactly the entry we most need to drop. With a
        // baseline it expires 3 days after we first heard of it unless it
        // actually syncs.
        if !newly_seeded.is_empty() {
            let mut seen = self.peer_seen.lock().await;
            let now = Self::now_ms();
            for peer in newly_seeded {
                seen.entry(peer).or_insert(PeerSeen { last_seen_ms: now, fail_rounds: 0 });
            }
        }
        if changed {
            self.persist_snapshot().await?;
            self.save_peer_seen().await;
        }
        Ok(())
    }

    /// All peer IDs recorded in the document (`peer:` prefix).
    async fn known_peers(&self) -> Result<Vec<EndpointId>> {
        let inner = self.inner.lock().await;
        let mut out = Vec::new();
        for key in inner.doc.keys(ROOT) {
            let key = key.to_string();
            if let Some(id) = key.strip_prefix(PEER_PREFIX) {
                if let Ok(id) = EndpointId::from_str(id) {
                    out.push(id);
                }
            }
        }
        Ok(out)
    }

    /// Drop the residual `in_flight` marker a peer's sync state may carry.
    ///
    /// `generate_sync_message` sets `in_flight` when it hands out a message,
    /// and only `receive_sync_message` clears it — when the peer's reply
    /// arrives. If an exchange ends any other way (the peer closed first, an
    /// I/O error, or our own timeout), the flag stays set and every later
    /// `generate_sync_message` returns `None`: the peer would sit silently
    /// "in sync" forever while sending and receiving nothing.
    ///
    /// Clearing it is safe because the protocol is a delta exchange: the next
    /// message is generated from current heads and the peer ignores changes it
    /// already has. This is called at the *start* of a round rather than after
    /// a failure because a timeout drops the sync future mid-await — code
    /// after that future's `.await` never runs.
    async fn reset_sync_state(&self, peer: EndpointId) {
        let mut inner = self.inner.lock().await;
        if let Some(state) = inner.sync_states.get_mut(&peer) {
            state.in_flight = false;
        }
    }

    /// Run the automerge sync protocol with a remote peer over one iroh bi-stream.
    ///
    /// Note: does NOT close the endpoint — `listen` nodes share one endpoint
    /// across gossip rounds and incoming connections. Callers that own a
    /// dedicated endpoint (the `connect` command) close it themselves.
    async fn sync_once(&self, endpoint: &Endpoint, peer: EndpointId) -> Result<()> {
        // A fresh round always starts clean: whatever the previous exchange did
        // for this peer, its connection is gone now. See `reset_sync_state`.
        self.reset_sync_state(peer).await;
        let conn = endpoint.connect(peer, ALPN).await.anyerr()?;
        let (mut send, mut recv) = conn.open_bi().await.anyerr()?;
        let res = self.run_sync(&mut send, &mut recv, peer).await;
        conn.close(0u32.into(), b"done");
        res
    }

    /// Sync loop against a single remote over provided streams.
    ///
    /// The automerge sync protocol is a lock-step message exchange; we hold
    /// the store lock only while generating or applying one message, never
    /// across an await.
    async fn run_sync(
        &self,
        send: &mut iroh::endpoint::SendStream,
        recv: &mut iroh::endpoint::RecvStream,
        peer: EndpointId,
    ) -> Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // We are the initiator: drive the exchange lock-step.
        self.refresh().await?;
        loop {
            // Generate one outgoing message (if any).
            let outgoing = {
                let mut inner = self.inner.lock().await;
                let StoreInner { doc, sync_states, .. } = &mut *inner;
                let state = sync_states.entry(peer).or_insert_with(sync::State::new);
                doc.sync().generate_sync_message(state)
            };
            let Some(message) = outgoing else {
                // Nothing more to send; tell the peer we're done.
                send.write_u32(0).await.anyerr()?;
                send.shutdown().await.anyerr()?;
                break;
            };
            let bytes = message.encode();
            send.write_u32(bytes.len() as u32).await.anyerr()?;
            send.write_all(&bytes).await.anyerr()?;

            // Read the peer's reply (length-prefixed). 0 = peer done.
            let len = recv.read_u32().await.anyerr()?;
            if len == 0 {
                break;
            }
            let mut buf = vec![0u8; len as usize];
            recv.read_exact(&mut buf).await.anyerr()?;
            let message = sync::Message::decode(&buf).anyerr()?;
            {
                let mut inner = self.inner.lock().await;
                let StoreInner { doc, sync_states, .. } = &mut *inner;
                let state = sync_states.entry(peer).or_insert_with(sync::State::new);
                doc.sync()
                    .receive_sync_message(state, message)
                    .anyerr()?;
            }
        }

        self.persist_snapshot().await?;
        Ok(())
    }
}

/// Protocol handler accepting incoming sync connections.
#[derive(Debug, Clone)]
struct SyncHandler(Store);

impl ProtocolHandler for SyncHandler {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let peer = connection.remote_id();
        let (mut send, mut recv) = connection
            .accept_bi()
            .await
            .map_err(|e| AcceptError::from_err(e))?;

        let store = self.0.clone();
        // The initiator drives; we just respond. Mirror their framing.
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            // Start every connection's exchange from a clean sync state. A
            // previous exchange that ended without the peer's reply (dropped
            // connection, our own read failure) leaves `in_flight` set, which
            // would make every `generate_sync_message` below return `None` and
            // leave us mute to this peer forever. See `reset_sync_state`.
            store.reset_sync_state(peer).await;
            let res: Result<()> = loop {
                // Pick up any changes written to disk by other processes.
                if let Err(e) = store.refresh().await {
                    break Err(e);
                }
                // Read one incoming message (0 = peer done).
                let len = match recv.read_u32().await {
                    Ok(0) | Err(_) => break Ok(()),
                    Ok(len) => len,
                };
                let mut buf = vec![0u8; len as usize];
                if recv.read_exact(&mut buf).await.is_err() {
                    break Ok(());
                }
                let Ok(message) = sync::Message::decode(&buf) else {
                    break Ok(());
                };
                {
                    let mut inner = store.inner.lock().await;
                    let StoreInner { doc, sync_states, .. } = &mut *inner;
                    let state = sync_states.entry(peer).or_insert_with(sync::State::new);
                    if let Err(e) = doc.sync().receive_sync_message(state, message) {
                        break Err(n0_error::AnyError::from_display(e));
                    }
                }
                // Reply with our next message, if any.
                let outgoing = {
                    let mut inner = store.inner.lock().await;
                    let StoreInner { doc, sync_states, .. } = &mut *inner;
                    let state = sync_states.entry(peer).or_insert_with(sync::State::new);
                    doc.sync().generate_sync_message(state)
                };
                match outgoing {
                    Some(message) => {
                        let bytes = message.encode();
                        if send.write_u32(bytes.len() as u32).await.is_err() {
                            break Ok(());
                        }
                        if send.write_all(&bytes).await.is_err() {
                            break Ok(());
                        }
                    }
                    None => {
                        let _ = send.write_u32(0).await;
                        let _ = send.shutdown().await;
                        break Ok(());
                    }
                }
            };
            if let Err(e) = res {
                tracing::warn!("sync with {peer} failed: {e}");
            }
            // Remember the peer in the document so it propagates to others.
            let _ = store.add_peers(std::slice::from_ref(&peer)).await;
            // An inbound sync proves the peer is alive, so refresh its
            // last-seen and keep it from being expired.
            store.mark_peer_seen(peer).await;
            store.save_peer_seen().await;
            let _ = store.persist_snapshot().await;
            connection.closed().await;
        });
        Ok(())
    }
}

impl Store {
    /// Merge any changes another process wrote to the on-disk document into
    /// our in-memory doc (no save).
    async fn refresh(&self) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let doc_path = self.data_dir.join("doc.automerge");
        if doc_path.exists() {
            let bytes = tokio::fs::read(&doc_path).await.anyerr()?;
            // Skip if this is our own last save (merging our own saved ops
            // back would fail with "duplicate seq" for the same actor).
            if inner.disk_len != Some(bytes.len() as u64) {
                let mut disk = AutoCommit::load(&bytes).anyerr()?;
                // Two different saves can share a length; if we do end up
                // re-merging our own ops, automerge rejects the duplicate —
                // harmless (we already have them), so ignore that error.
                if inner.doc.merge(&mut disk).is_ok() {
                    inner.disk_len = Some(bytes.len() as u64);
                }
            }
        }
        Ok(())
    }

    async fn persist_snapshot(&self) -> Result<()> {
        // Hold the persist lock across the whole read-merge-write sequence:
        // concurrent sync tasks each call this, and without serialising it
        // two of them can read the same on-disk state and save over each other
        // (losing one side's document updates), even though the map writes
        // themselves go through `inner`.
        let _guard = self.persist_lock.lock().await;
        let mut doc = {
            let mut inner = self.inner.lock().await;
            // Merge in any changes another process (e.g. a CLI invocation)
            // wrote to the on-disk document since we loaded it, so we never
            // clobber concurrent local writes. Skip files we wrote ourselves;
            // a coincidental same-length re-merge of our own ops is harmless.
            let doc_path = self.data_dir.join("doc.automerge");
            if doc_path.exists() {
                let bytes = tokio::fs::read(&doc_path).await.anyerr()?;
                if inner.disk_len != Some(bytes.len() as u64) {
                    let mut disk = AutoCommit::load(&bytes).anyerr()?;
                    let _ = inner.doc.merge(&mut disk);
                }
            }
            inner.doc.clone()
        };
        // Record the length we are about to write so the next refresh skips it.
        {
            let mut inner = self.inner.lock().await;
            inner.disk_len = Some(doc.save().len() as u64);
        }
        self.persist(&mut doc).await
    }

    /// Record that we just synced successfully with `peer`. Local only — this
    /// timestamp is what the 1-day expiry is measured against. Clears the
    /// fail-round counter so a flaky peer does not get dropped mid-recovery.
    async fn mark_peer_seen(&self, peer: EndpointId) {
        let mut seen = self.peer_seen.lock().await;
        seen.insert(
            peer,
            PeerSeen { last_seen_ms: Self::now_ms(), fail_rounds: 0 },
        );
    }

    /// Start the expiry clock for peers we are about to dial but have no
    /// last-seen for. Peers arrive mostly by syncing the document (their
    /// `peer:` keys propagate), which does not go through `add_peers`, so
    /// without this they would have no baseline and never expire.
    async fn seed_peer_baselines(&self, peers: &[EndpointId]) {
        let mut seen = self.peer_seen.lock().await;
        let now = Self::now_ms();
        for peer in peers {
            seen.entry(*peer).or_insert(PeerSeen { last_seen_ms: now, fail_rounds: 0 });
        }
    }

    /// Record that a gossip round failed to reach `peer`. Cumulative — the
    /// counter is reset on every successful sync by [`mark_peer_seen`].
    /// Bumping is best-effort: if the peer has no row yet, seed it with
    /// 1 fail round so it has a baseline to count from.
    async fn bump_peer_fail(&self, peer: EndpointId) {
        let mut seen = self.peer_seen.lock().await;
        seen.entry(peer)
            .and_modify(|p| p.fail_rounds = p.fail_rounds.saturating_add(1))
            .or_insert(PeerSeen { last_seen_ms: Self::now_ms(), fail_rounds: 1 });
    }

    /// Persist the last-seen table to disk so the expiry baseline survives a
    /// restart (otherwise every restart would look like "never seen anyone").
    async fn save_peer_seen(&self) {
        let snapshot: std::collections::HashMap<String, PeerSeen> = {
            let seen = self.peer_seen.lock().await;
            seen.iter().map(|(k, v)| (k.to_string(), *v)).collect()
        };
        let path = self.data_dir.join(PEER_SEEN_FILE);
        // Unique temp name, for the same reason as `persist`: several tasks
        // (each inbound connection, each gossip round) call this concurrently.
        let n = self.tmp_seq.fetch_add(1, Ordering::Relaxed);
        let tmp = self.data_dir.join(format!("{PEER_SEEN_FILE}.{n}.tmp"));
        if let Ok(bytes) = serde_json::to_vec(&snapshot)
            && tokio::fs::write(&tmp, bytes).await.is_ok()
        {
            let _ = tokio::fs::rename(&tmp, &path).await;
        }
    }

    /// Drop peers from the document (and the local last-seen table) so they
    /// stop being dialled. Automerge removes propagate through sync like any
    /// other change, reaching the rest of the swarm.
    async fn remove_peers(&self, peers: &[EndpointId]) -> Result<()> {
        if peers.is_empty() {
            return Ok(());
        }
        // Mutate the in-memory doc first, then let `persist_snapshot` take the
        // persist lock itself. Do not hold it here: the lock is not reentrant,
        // and `persist_snapshot` needs it for its own read-merge-write.
        {
            let mut inner = self.inner.lock().await;
            for peer in peers {
                let key = format!("{PEER_PREFIX}{peer}");
                inner.doc.delete(ROOT, &key).anyerr()?;
            }
        }
        self.persist_snapshot().await?;
        {
            let mut seen = self.peer_seen.lock().await;
            for peer in peers {
                seen.remove(peer);
            }
        }
        self.save_peer_seen().await;
        Ok(())
    }

    /// Expire peers that have either gone quiet for too long (reactive —
    /// [`PEER_TTL`]) or accumulated too many consecutive failed dials
    /// (proactive — [`ZOMBIE_FAIL_ROUNDS`]). The proactive half is the
    /// important one for this codebase: peers arrive in batches via document
    /// sync, and a freshly-learned zombie stays on the dial list for the
    /// full TTL otherwise, slowing every gossip round.
    ///
    /// A peer with no recorded last-seen is left alone: we have no
    /// observation to justify dropping it, and dropping it would risk
    /// pruning a node that is simply not yet reachable from here.
    async fn expire_stale_peers(&self, self_id: EndpointId) {
        let cutoff = Self::now_ms().saturating_sub(PEER_TTL.as_millis() as u64);
        let mut stale: Vec<EndpointId> = {
            let seen = self.peer_seen.lock().await;
            seen.iter()
                .filter_map(|(id, p)| {
                    if *id == self_id {
                        return None;
                    }
                    let ttl_expired = p.last_seen_ms != 0 && p.last_seen_ms < cutoff;
                    let zombie = p.fail_rounds >= ZOMBIE_FAIL_ROUNDS;
                    if ttl_expired || zombie { Some(*id) } else { None }
                })
                .collect()
        };

        // Cap the peer list. If we accumulated more than MAX_PEERS, evict
        // the entries with the oldest last-seen (the ones least worth keeping).
        // Self is always kept.
        {
            let seen = self.peer_seen.lock().await;
            if seen.len() > MAX_PEERS {
                let mut by_age: Vec<(EndpointId, u64)> = seen
                    .iter()
                    .map(|(id, p)| (*id, p.last_seen_ms))
                    .collect();
                by_age.sort_by_key(|(_, ts)| *ts);
                let excess = seen.len() - MAX_PEERS;
                for (id, _) in by_age.iter().take(excess) {
                    if *id != self_id && !stale.contains(id) {
                        stale.push(*id);
                    }
                }
            }
        }

        if stale.is_empty() {
            return;
        }
        for peer in &stale {
            tracing::info!("expiring peer {peer}: no recent sync (ttl or fail-round cap)");
        }
        if let Err(e) = self.remove_peers(&stale).await {
            tracing::warn!("failed to remove expired peers: {e}");
        }
    }
}

async fn print_record(store: &Store, record_id: &str) {
    if let Ok(Some(rec)) = store.read_record(record_id).await {
        println!("{rec}");
    }
}

fn as_f64(v: &automerge::Value) -> Result<f64> {
    match v {
        automerge::Value::Scalar(s) => match s.as_ref() {
            automerge::ScalarValue::F64(f) => Ok(*f),
            automerge::ScalarValue::Uint(u) => Ok(*u as f64),
            automerge::ScalarValue::Int(i) => Ok(*i as f64),
            other => Err(anyerr!("expected number, got {other:?}")),
        },
        other => Err(anyerr!("expected scalar, got {other:?}")),
    }
}

/// Format unix seconds as `(date, time, utc-offset)` in local time, e.g.
/// `("2026-10-14", "15:00:00", "+08:00")`. Uses the Windows API through the
/// standard library's local-time offset only via `chrono`-free math: we take
/// the local offset once from the `system_clock` — simplest correct approach
/// without a timezone database is to compute the offset from the local time
/// the OS reports for "now".
fn civil_from_unix(secs: u64) -> (String, String, String) {
    // Determine the local offset by asking the filesystem-free std API:
    // std has no local-timezone support, so use the offset at compile-time
    // zone via `SystemTime` trick — instead, compute UTC and append the
    // offset measured from the OS at runtime via the TZ env or fallback +00:00.
    //
    // For simplicity and determinism we emit UTC (+00:00) unless the
    // `M2_TZ_OFFSET_MINUTES` env var is set (e.g. 480 for UTC+8).
    let offset_min = std::env::var("M2_TZ_OFFSET_MINUTES")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);

    let local = secs as i64 + offset_min * 60;
    let days = local.div_euclid(86_400);
    let rem = local.rem_euclid(86_400);
    let (h, m, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    // Civil-from-days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if mo <= 2 { y + 1 } else { y };

    let sign = if offset_min < 0 { '-' } else { '+' };
    let oh = offset_min.abs() / 60;
    let om = offset_min.abs() % 60;
    (
        format!("{y:04}-{mo:02}-{d:02}"),
        format!("{h:02}:{m:02}:{s:02}"),
        format!("{sign}{oh:02}:{om:02}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A unique scratch data dir per test, cleaned up on drop.
    struct TempDir(PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            let n = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let p = std::env::temp_dir().join(format!("m2p2p-test-{tag}-{n}"));
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Build a sync-only endpoint for tests.
    ///
    /// Tests don't need the production hardening (no DnsAddressLookup, no
    /// pkarr address-lookups), so they use the N0 preset which selects a
    /// rustls crypto provider for them. The TCP-DNS burst this avoids is
    /// a long-running production concern; in a 10-second unit test it
    /// never has a chance to manifest.
    async fn build_test_endpoint(key: SecretKey) -> iroh::Endpoint {
        Endpoint::builder(presets::N0)
            .secret_key(key)
            .bind()
            .await
            .unwrap()
    }

    /// A syntactically valid, distinct endpoint ID per `seed`.
    fn peer(seed: u8) -> EndpointId {
        let key = SecretKey::from_bytes(&[seed; 32]);
        key.public()
    }

    #[tokio::test]
    async fn secret_key_is_stable_across_reopen() {
        let dir = TempDir::new("key");
        let first = load_or_create_secret_key(dir.path()).await.unwrap();
        let second = load_or_create_secret_key(dir.path()).await.unwrap();
        assert_eq!(
            first.public(),
            second.public(),
            "endpoint ID must survive a restart, or peers keep dialing a dead ID"
        );
        // Persisted as exactly 32 raw bytes.
        let raw = std::fs::read(dir.path().join(KEY_FILE)).unwrap();
        assert_eq!(raw.len(), 32);
    }

    #[tokio::test]
    async fn corrupt_secret_key_is_rejected() {
        let dir = TempDir::new("key-bad");
        std::fs::write(dir.path().join(KEY_FILE), b"not-32-bytes").unwrap();
        assert!(load_or_create_secret_key(dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn peers_older_than_ttl_are_expired() {
        let dir = TempDir::new("expire");
        let store = Store::open(&dir.0).await.unwrap();
        let me = peer(1);
        let fresh = peer(2);
        let zombie = peer(3);

        store.add_peers(&[me, fresh, zombie]).await.unwrap();
        // `fresh` synced a moment ago; `zombie` was last seen 4 days ago.
        store.peer_seen.lock().await.insert(
            zombie,
            PeerSeen {
                last_seen_ms: Store::now_ms() - (PEER_TTL.as_millis() as u64) - 1000,
                fail_rounds: 0,
            },
        );

        assert_eq!(store.known_peers().await.unwrap().len(), 3);
        store.expire_stale_peers(me).await;

        let remaining = store.known_peers().await.unwrap();
        assert!(!remaining.contains(&zombie), "zombie peer must be dropped");
        assert!(remaining.contains(&fresh), "recently seen peer must be kept");
        assert!(remaining.contains(&me), "we must never expire ourselves");
    }

    #[tokio::test]
    async fn never_synced_peer_still_gets_a_baseline_and_expires() {
        // The zombie case: we learned a peer's ID but never once reached it.
        // It must still have an expiry clock, or it would live forever.
        let dir = TempDir::new("baseline");
        let store = Store::open(&dir.0).await.unwrap();
        let me = peer(1);
        let unreachable = peer(4);

        // Learned from the document, not via add_peers.
        store.seed_peer_baselines(&[unreachable]).await;
        assert!(store.peer_seen.lock().await.contains_key(&unreachable));

        // Age it past the TTL.
        store.peer_seen.lock().await.insert(
            unreachable,
            PeerSeen {
                last_seen_ms: Store::now_ms() - (PEER_TTL.as_millis() as u64) - 1000,
                fail_rounds: 0,
            },
        );
        store.add_peers(&[unreachable]).await.unwrap();
        store.expire_stale_peers(me).await;
        assert!(!store.known_peers().await.unwrap().contains(&unreachable));
    }

    #[tokio::test]
    async fn concurrent_persist_keeps_every_write() {
        // Regression guard for the temp-file collision: many concurrent sync
        // tasks each persist; no save may silently clobber another.
        let dir = TempDir::new("persist");
        let store = Store::open(&dir.0).await.unwrap();
        let mut tasks = Vec::new();
        for i in 0..16u8 {
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                let id = Uuid::new_v4().to_string();
                s.upsert_record(
                    &id,
                    &RecordUpsert {
                        ghname: format!("u{i}"),
                        loginname: "l".into(),
                        cp_aic: i as f64,
                        device_id: "local".into(),
                        device_name: "d".into(),
                        info1: String::new(),
                        info2: String::new(),
                    },
                )
                .await
                .unwrap();
            }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        // Reopen from disk: every record must have survived the concurrent saves.
        let reopened = Store::open(&dir.0).await.unwrap();
        assert_eq!(reopened.list_records().await.unwrap().len(), 16);
    }

    #[tokio::test]
    async fn gossip_syncs_all_peers_concurrently_despite_one_dead() {
        // The core fix: an unreachable peer must not stop the others, and must
        // not hold the round for longer than its own timeout.
        use iroh::protocol::Router;

        let dir_a = TempDir::new("swarm-a");
        let dir_b = TempDir::new("swarm-b");
        let store_b = Store::open(&dir_b.0).await.unwrap();
        let key_b = load_or_create_secret_key(dir_b.path()).await.unwrap();
        let ep_b = build_test_endpoint(key_b).await;
        let router_b = Router::builder(ep_b.clone())
            .accept(ALPN, SyncHandler(store_b.clone()))
            .spawn();
        ep_b.online().await;

        let store_a = Store::open(&dir_a.0).await.unwrap();
        let key_a = load_or_create_secret_key(dir_a.path()).await.unwrap();
        let ep_a = build_test_endpoint(key_a).await;

        // Give B a record A does not have yet.
        let rid = Uuid::new_v4().to_string();
        store_b
            .upsert_record(
                &rid,
                &RecordUpsert {
                    ghname: "b".into(),
                    loginname: "b".into(),
                    cp_aic: 1.0,
                    device_id: "local".into(),
                    device_name: "B".into(),
                    info1: String::new(),
                    info2: String::new(),
                },
            )
            .await
            .unwrap();

        // A knows one real peer (B) and one dead one (a random key nobody runs).
        let dead = peer(99);
        let started = std::time::Instant::now();
        let (live, _dead) = tokio::join!(
            tokio::time::timeout(SYNC_TIMEOUT, store_a.sync_once(&ep_a, ep_b.id())),
            tokio::time::timeout(SYNC_TIMEOUT, store_a.sync_once(&ep_a, dead)),
        );
        assert!(live.is_ok() && live.unwrap().is_ok(), "live peer must sync");

        // The live sync must complete promptly, not wait on the dead peer's
        // full timeout — that serial stall is what starved the swarm before.
        assert!(
            started.elapsed() < SYNC_TIMEOUT,
            "live sync must not be blocked by the unreachable peer"
        );
        assert!(
            store_a.read_record(&rid).await.unwrap().is_some(),
            "record must have arrived from B and persisted"
        );

        router_b.shutdown().await.unwrap();
        ep_a.close().await;
    }

    #[tokio::test]
    async fn a_timed_out_sync_does_not_latch_in_flight() {
        // Regression guard for a bug the first cut of the timeout work had:
        // the fix-up ran *after* `sync_once().await`, so a timeout — which
        // drops that future mid-await — skipped it entirely. The peer's sync
        // state then kept `in_flight = true` forever, and since
        // `generate_sync_message` returns `None` while it is set, that peer
        // was silently mute for the life of the process.
        //
        // Reproduce the exact condition: leave a state marked in_flight, then
        // run a round that times out, and assert the state comes out clean.
        use iroh::protocol::Router;

        /// Accepts the stream but never replies, so the initiator's read
        /// blocks until its own timeout fires — the timeout path we are
        /// guarding, as opposed to an ordinary connection error.
        #[derive(Debug)]
        struct HangingHandler;
        impl ProtocolHandler for HangingHandler {
            async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
                let (_send, _recv) = connection.accept_bi().await.map_err(AcceptError::from_err)?;
                // Hold the connection open forever without answering.
                std::future::pending::<Result<(), AcceptError>>().await
            }
        }

        let dir_peer = TempDir::new("dead-peer");
        let key_peer = load_or_create_secret_key(dir_peer.path()).await.unwrap();
        let ep_peer = build_test_endpoint(key_peer).await;
        let router_peer = Router::builder(ep_peer.clone())
            .accept(ALPN, HangingHandler)
            .spawn();
        ep_peer.online().await;

        let dir_a = TempDir::new("dead-initiator");
        let store_a = Store::open(&dir_a.0).await.unwrap();
        let key_a = load_or_create_secret_key(dir_a.path()).await.unwrap();
        let ep_a = build_test_endpoint(key_a).await;

        let target = ep_peer.id();
        // Simulate the residue a previous cut-short exchange leaves behind.
        store_a
            .inner
            .lock()
            .await
            .sync_states
            .entry(target)
            .or_insert_with(sync::State::new)
            .in_flight = true;

        // A round against a peer that goes silent must time out...
        let short = Duration::from_millis(500);
        let r = tokio::time::timeout(short, store_a.sync_once(&ep_a, target)).await;
        assert!(r.is_err(), "sync should have hit the timeout");

        // ...and must leave the state usable, so the next round can send again.
        let latched = store_a
            .inner
            .lock()
            .await
            .sync_states
            .get(&target)
            .map(|s| s.in_flight);
        assert_eq!(
            latched,
            Some(false),
            "in_flight must be cleared for a timed-out peer, or it goes mute forever"
        );

        router_peer.shutdown().await.unwrap();
        ep_a.close().await;
    }

    #[test]
    fn expiry_is_skipped_when_no_peer_was_reachable() {
        // A node offline for >PEER_TTL fails every sync; without this guard it
        // would delete its whole (healthy) peer list from the shared document
        // and could never recover it. See `may_expire_peers`.
        assert!(!may_expire_peers(5, false), "offline node must not purge peers");
        assert!(may_expire_peers(5, true), "a round that reached someone may expire");
        // No peers to dial at all (fresh node): nothing to protect against.
        assert!(may_expire_peers(0, false));
    }

    #[tokio::test]
    async fn expire_stale_peers_only_touches_the_stale_ones() {
        let dir = TempDir::new("mixed");
        let store = Store::open(&dir.0).await.unwrap();
        let me = peer(1);
        let alive = peer(2);
        let gone = peer(3);

        store.add_peers(&[me, alive, gone]).await.unwrap();
        {
            let mut seen = store.peer_seen.lock().await;
            let old_ms = Store::now_ms() - (PEER_TTL.as_millis() as u64) - 1000;
            seen.insert(gone, PeerSeen { last_seen_ms: old_ms, fail_rounds: 0 });
        }
        store.expire_stale_peers(me).await;

        let remaining = store.known_peers().await.unwrap();
        assert!(remaining.contains(&alive), "a recently seen peer must survive");
        assert!(!remaining.contains(&gone), "the stale peer must be dropped");
        assert!(remaining.contains(&me));
    }
}
