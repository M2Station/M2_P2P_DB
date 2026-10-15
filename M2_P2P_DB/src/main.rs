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
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use automerge::{
    AutoCommit, ObjType, ReadDoc, ROOT,
    sync::{self, SyncDoc},
    transaction::Transactable,
};
use clap::{Parser, Subcommand};
use iroh::{
    Endpoint, EndpointId,
    endpoint::{Connection, presets},
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
            let endpoint = Endpoint::bind(presets::N0).await?;
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
    }
}

async fn run_listen(store: Store, peers: Vec<EndpointId>) -> Result<()> {
    let endpoint = Endpoint::bind(presets::N0).await?;
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
    let mut ticker = tokio::time::interval(Duration::from_secs(30));
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
            for peer in known {
                if peer == gossip_endpoint.id() {
                    continue;
                }
                if let Err(e) = gossip_store.sync_once(&gossip_endpoint, peer).await {
                    tracing::debug!("gossip sync with {peer} failed: {e}");
                }
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

/// Local document store: automerge doc + persistence + sync helpers.
#[derive(Debug, Clone)]
struct Store {
    inner: std::sync::Arc<Mutex<StoreInner>>,
    data_dir: PathBuf,
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
        Ok(Self {
            inner: std::sync::Arc::new(Mutex::new(StoreInner {
                doc,
                sync_states: Default::default(),
                disk_len,
            })),
            data_dir: data_dir.clone(),
        })
    }

    async fn persist(&self, doc: &mut AutoCommit) -> Result<()> {
        let path = self.data_dir.join("doc.automerge");
        let bytes = doc.save();
        // Write to a temp file then rename for basic crash safety.
        let tmp = self.data_dir.join("doc.automerge.tmp");
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
        {
            let mut inner = self.inner.lock().await;
            for peer in peers {
                let key = format!("{PEER_PREFIX}{peer}");
                if inner.doc.get(ROOT, &key).anyerr()?.is_none() {
                    inner.doc.put(ROOT, &key, Self::now_ms()).anyerr()?;
                    changed = true;
                }
            }
        }
        if changed {
            self.persist_snapshot().await?;
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

    /// Run the automerge sync protocol with a remote peer over one iroh bi-stream.
    ///
    /// Note: does NOT close the endpoint — `listen` nodes share one endpoint
    /// across gossip rounds and incoming connections. Callers that own a
    /// dedicated endpoint (the `connect` command) close it themselves.
    async fn sync_once(&self, endpoint: &Endpoint, peer: EndpointId) -> Result<()> {
        let conn = endpoint.connect(peer, ALPN).await.anyerr()?;
        let (mut send, mut recv) = conn.open_bi().await.anyerr()?;
        self.run_sync(&mut send, &mut recv, peer).await?;
        conn.close(0u32.into(), b"done");
        Ok(())
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
                inner.doc.merge(&mut disk).anyerr()?;
                inner.disk_len = Some(bytes.len() as u64);
            }
        }
        Ok(())
    }

    async fn persist_snapshot(&self) -> Result<()> {
        let mut doc = {
            let mut inner = self.inner.lock().await;
            // Merge in any changes another process (e.g. a CLI invocation)
            // wrote to the on-disk document since we loaded it, so we never
            // clobber concurrent local writes. Skip files we wrote ourselves.
            let doc_path = self.data_dir.join("doc.automerge");
            if doc_path.exists() {
                let bytes = tokio::fs::read(&doc_path).await.anyerr()?;
                if inner.disk_len != Some(bytes.len() as u64) {
                    let mut disk = AutoCommit::load(&bytes).anyerr()?;
                    inner.doc.merge(&mut disk).anyerr()?;
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
