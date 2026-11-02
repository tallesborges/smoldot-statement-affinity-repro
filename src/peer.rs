//! A loopback full node for one chain, built from the networking state machine
//! of the `smoldot` crate (`smoldot::network::service::ChainNetwork`), the same
//! state machine smoldot-light runs. It accepts the light client's TCP
//! connection, accepts its gossip links (which open the statement protocol),
//! records every topic-affinity filter the light client sends, and on request
//! sends signed-shaped statements back.
//!
//! It is a test peer, not a full node: it keeps no statement store and does not
//! verify signatures.

use std::{
    collections::HashMap,
    net::TcpListener,
    pin::Pin,
    sync::{Arc, Mutex, mpsc},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::smoldot::{
    libp2p::with_buffers::WithBuffers,
    network::{
        codec::{self, AffinityFilter, Proof, Statement, StatementProtocolVersion},
        service::{
            self, ChainConfig, ChainNetwork, ConnectionId, NoiseKey, PeerId, Role,
            SingleStreamConnectionTask, SingleStreamHandshakeKind,
        },
    },
};
use futures_lite::future;

type Socket = WithBuffers<
    future::Ready<std::io::Result<smol::net::TcpStream>>,
    smol::net::TcpStream,
    Instant,
>;

struct Connection {
    id: ConnectionId,
    task: Option<SingleStreamConnectionTask<Instant>>,
    socket: Pin<Box<Socket>>,
}

/// A topic-affinity filter as received on the wire.
#[derive(Clone)]
pub struct ReceivedFilter {
    pub from: String,
    pub filter: AffinityFilter,
}

/// What the peer has seen, shared with the scenarios.
#[derive(Default)]
pub struct Observed {
    pub filters: Vec<ReceivedFilter>,
    /// Light-client peers whose statement substream the peer opened, oldest first.
    pub statement_peers: Vec<String>,
    pub log: Vec<String>,
}

/// How the peer chooses which statements to send.
#[derive(Clone, Copy)]
pub enum Forwarding {
    /// Send every statement, whatever the receiver advertised.
    IgnoreFilter,
    /// Send a statement only if the receiver's latest filter matches it, as
    /// polkadot-sdk's statement protocol does for a light peer.
    HonorLatestFilter,
}

pub struct SendStatements {
    pub to: String,
    pub statements: Vec<([u8; 32], String)>,
    pub forwarding: Forwarding,
    pub reply: mpsc::Sender<Vec<String>>,
}

pub struct Peer {
    pub multiaddr: String,
    pub observed: Arc<Mutex<Observed>>,
    commands: mpsc::Sender<SendStatements>,
}

impl Peer {
    /// Returns one line per statement: sent, or withheld and why.
    pub fn send(
        &self,
        to: &str,
        statements: Vec<([u8; 32], String)>,
        forwarding: Forwarding,
    ) -> Vec<String> {
        let (reply, answer) = mpsc::channel();
        self.commands
            .send(SendStatements {
                to: to.to_owned(),
                statements,
                forwarding,
                reply,
            })
            .unwrap();
        answer
            .recv_timeout(Duration::from_secs(5))
            .expect("peer did not answer")
    }
}

pub fn spawn(genesis_hash: [u8; 32]) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();

    let noise_key = NoiseKey::new(&[7u8; 32], &[9u8; 32]);
    let peer_id = peer_id_of(&noise_key);
    let multiaddr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer_id}");

    let observed = Arc::new(Mutex::new(Observed::default()));
    let (commands, commands_rx) = mpsc::channel();
    let shared = observed.clone();
    std::thread::spawn(move || {
        smol::block_on(run(listener, noise_key, genesis_hash, shared, commands_rx))
    });
    Peer {
        multiaddr,
        observed,
        commands,
    }
}

fn peer_id_of(noise_key: &NoiseKey) -> PeerId {
    service::peer_id::PublicKey::Ed25519(*noise_key.libp2p_public_ed25519_key()).into_peer_id()
}

async fn run(
    listener: TcpListener,
    noise_key: NoiseKey,
    genesis_hash: [u8; 32],
    observed: Arc<Mutex<Observed>>,
    commands: mpsc::Receiver<SendStatements>,
) {
    let started = Instant::now();
    let mut network = ChainNetwork::<(), (), Instant>::new(service::Config {
        connections_capacity: 8,
        chains_capacity: 1,
        randomness_seed: [3u8; 32],
        handshake_timeout: Duration::from_secs(8),
    });
    let chain_id = network
        .add_chain(ChainConfig {
            user_data: (),
            genesis_hash,
            fork_id: None,
            block_number_bytes: 4,
            grandpa_protocol_config: None,
            allow_inbound_block_requests: false,
            best_hash: genesis_hash,
            best_number: 0,
            role: Role::Full,
            enable_statement_protocol: true,
        })
        .unwrap();

    // `ChainNetwork` indexes a connection by peer only if it was added with an
    // expected peer id, and `gossip_open` needs that index. An inbound
    // connection has no expected id, so a placeholder is given; the handshake
    // replaces it with the real one.
    let placeholder = peer_id_of(&NoiseKey::new(&[1u8; 32], &[2u8; 32]));

    let mut connections: Vec<Connection> = Vec::new();
    let mut latest_filter: HashMap<String, AffinityFilter> = HashMap::new();
    let mut statement_peers: HashMap<String, PeerId> = HashMap::new();
    let mut sequence: u32 = 0;
    let note = |text: String| {
        let at = started.elapsed().as_secs_f64();
        observed
            .lock()
            .unwrap()
            .log
            .push(format!("[{at:>6.2}s] {text}"));
    };

    loop {
        let mut busy = false;

        while let Ok((stream, remote)) = listener.accept() {
            busy = true;
            let stream = smol::net::TcpStream::try_from(stream).unwrap();
            let _ = stream.set_nodelay(true);
            let (id, task) = network.add_single_stream_connection(
                Instant::now(),
                SingleStreamHandshakeKind::MultistreamSelectNoiseYamux {
                    is_initiator: false,
                    noise_key: &noise_key,
                },
                format!("/ip4/{}/tcp/{}", remote.ip(), remote.port()).into_bytes(),
                Some(placeholder.clone()),
                (),
            );
            note(format!("tcp connection from {remote}"));
            connections.push(Connection {
                id,
                task: Some(task),
                socket: Box::pin(WithBuffers::new(future::ready(Ok(stream)))),
            });
        }

        for connection in &mut connections {
            let _ = future::poll_once(connection.socket.as_mut().wait_read_write_again(
                |when| async move {
                    smol::Timer::at(when).await;
                },
            ))
            .await;

            let Some(mut task) = connection.task.take() else {
                continue;
            };
            match connection.socket.as_mut().read_write_access(Instant::now()) {
                Ok(mut read_write) => {
                    let before = (read_write.read_bytes, read_write.write_bytes_queued);
                    task.read_write(&mut read_write);
                    busy |= before != (read_write.read_bytes, read_write.write_bytes_queued);
                }
                Err(_) => {
                    if !task.is_reset_called() {
                        task.reset();
                    }
                }
            }
            let (task, message) = task.pull_message_to_coordinator();
            connection.task = task;
            if let Some(message) = message {
                busy = true;
                network.inject_connection_message(connection.id, message);
            }
        }
        connections.retain(|connection| connection.task.is_some());

        while let Some((id, message)) = network.pull_message_to_connection() {
            busy = true;
            if let Some(task) = connections
                .iter_mut()
                .find(|connection| connection.id == id)
                .and_then(|connection| connection.task.as_mut())
            {
                task.inject_coordinator_message(&Instant::now(), message);
            }
        }

        while let Some(event) = network.next_event() {
            busy = true;
            match event {
                service::Event::HandshakeFinished { peer_id, .. } => {
                    note(format!("handshake finished with {peer_id}"));
                }
                service::Event::GossipInDesired {
                    peer_id,
                    chain_id: wanted,
                    kind,
                } => {
                    note(format!("gossip link requested by {peer_id}; accepting"));
                    if let Err(error) = network.gossip_open(wanted, &peer_id, kind) {
                        note(format!("gossip_open failed: {error:?}"));
                    }
                }
                service::Event::StatementProtocolConnected {
                    peer_id, version, ..
                } => {
                    let version = match version {
                        StatementProtocolVersion::V1 => "V1",
                        StatementProtocolVersion::V2 => "V2",
                    };
                    note(format!(
                        "statement protocol {version} open towards {peer_id}"
                    ));
                    observed
                        .lock()
                        .unwrap()
                        .statement_peers
                        .push(peer_id.to_string());
                    statement_peers.insert(peer_id.to_string(), peer_id);
                }
                service::Event::StatementTopicAffinityReceived {
                    peer_id, filter, ..
                } => {
                    note(format!("topic affinity filter received from {peer_id}"));
                    latest_filter.insert(peer_id.to_string(), filter.clone());
                    observed.lock().unwrap().filters.push(ReceivedFilter {
                        from: peer_id.to_string(),
                        filter,
                    });
                }
                service::Event::IdentifyRequestIn { substream_id, .. } => {
                    network
                        .respond_identify(substream_id, "statement-affinity-repro loopback peer");
                }
                service::Event::Disconnected { peer_id, .. } => {
                    note(format!("connection closed with {peer_id}"));
                    statement_peers.remove(&peer_id.to_string());
                }
                _ => {}
            }
        }

        while let Ok(command) = commands.try_recv() {
            busy = true;
            let mut lines = Vec::new();
            let expiry_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 3600;
            for (topic, label) in &command.statements {
                sequence += 1;
                let statement = Statement {
                    // smoldot-light delivers a received statement only if it
                    // carries a proof and has not expired; it does not verify
                    // the signature.
                    proof: Some(Proof::Ed25519 {
                        signature: [0u8; 64],
                        signer: [1u8; 32],
                    }),
                    decryption_key: None,
                    expiry: (expiry_secs << 32) | u64::from(sequence),
                    channel: None,
                    topics: vec![*topic],
                    data: Some(label.as_bytes().to_vec()),
                };
                let Some(target) = statement_peers.get(&command.to) else {
                    lines.push(format!("{label}: no statement substream with the client"));
                    continue;
                };
                let allowed = match command.forwarding {
                    Forwarding::IgnoreFilter => true,
                    Forwarding::HonorLatestFilter => latest_filter
                        .get(&command.to)
                        .is_some_and(|filter| filter.matches_statement(&[topic])),
                };
                if !allowed {
                    lines.push(format!(
                        "{label}: withheld, not in the client's latest filter"
                    ));
                    continue;
                }
                let encoded = codec::encode_statement(&statement).unwrap();
                match network.gossip_send_statement(target, chain_id, encoded) {
                    Ok(()) => lines.push(format!("{label}: sent")),
                    Err(error) => lines.push(format!("{label}: send failed: {error:?}")),
                }
            }
            let _ = command.reply.send(lines);
        }

        if !busy {
            smol::Timer::after(Duration::from_millis(2)).await;
        }
    }
}
