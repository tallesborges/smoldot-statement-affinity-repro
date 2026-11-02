//! Offline reproduction: on one smoldot-light `Client`, two `add_chain` calls
//! with the same chain spec give two chain handles. smoldot runs one network
//! service for both, and a separate JSON-RPC service per handle. Each JSON-RPC
//! service advertises a statement topic-affinity filter built from its own
//! subscriptions only, and the network service replaces the chain's filter with
//! whichever arrived last. Peers that honor the filter then stop sending
//! statements that the other handle is still subscribed to.
//!
//! Exit status: 0 when every check passes (the behavior this repro expects),
//! 1 when any check fails (the bug is reproduced).

#[cfg(all(feature = "smoldot-light-3", feature = "smoldot-light-2"))]
compile_error!("enable exactly one of the `smoldot-light-3` and `smoldot-light-2` features");

#[cfg(feature = "smoldot-light-2")]
use {smoldot_4 as smoldot, smoldot_light_2 as smoldot_light};
#[cfg(feature = "smoldot-light-3")]
use {smoldot_5 as smoldot, smoldot_light_3 as smoldot_light};

#[cfg(feature = "smoldot-light-2")]
const SMOLDOT_LIGHT_VERSION: &str = "2.1.0";
#[cfg(feature = "smoldot-light-3")]
const SMOLDOT_LIGHT_VERSION: &str = "3.0.0";

mod peer;

use std::{
    cell::Cell,
    iter,
    num::NonZero,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use serde_json::{Value, json};
use smoldot::network::codec::{self, AffinityFilter};
use smoldot_light::{
    AddChainConfig, AddChainConfigJsonRpc, ChainId, Client, StatementProtocolConfig,
    platform::DefaultPlatform,
};

use peer::Forwarding;

type LightClientInner = Client<Arc<DefaultPlatform>, ()>;

/// Fetched by `fetch-relay-spec.sh`, verbatim from smoldot's repository.
const RELAY_SPEC_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/specs/relay-paseo.json");
const PARACHAIN_SPEC: &str = include_str!("../specs/parachain.json");

/// smoldot's own example setting; at most one filter per chain handle per interval.
const AFFINITY_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const FILTER_WAIT: Duration = Duration::from_secs(6);
/// How long to wait to conclude that no filter is coming.
const NO_FILTER_WAIT: Duration = Duration::from_secs(3);

const TOPIC_A: [u8; 32] = [0xA1; 32];
const TOPIC_B: [u8; 32] = [0xB2; 32];
const TOPIC_C: [u8; 32] = [0xC3; 32];

/// Fixed bloom seed per chain handle, so every run is identical and each
/// filter on the wire shows which handle's JSON-RPC service built it.
const SEED_HANDLE_1: u128 = 0x1111_1111_1111_1111_1111_1111_1111_1111;
const SEED_HANDLE_2: u128 = 0x2222_2222_2222_2222_2222_2222_2222_2222;

fn topic_name(topic: &[u8; 32]) -> &'static str {
    match *topic {
        TOPIC_A => "A",
        TOPIC_B => "B",
        TOPIC_C => "C",
        _ => "?",
    }
}

fn author(filter: &AffinityFilter) -> &'static str {
    let seed = u128::from_le_bytes(filter.encode_to_vec()[..16].try_into().unwrap());
    match seed {
        SEED_HANDLE_1 => "handle 1",
        SEED_HANDLE_2 => "handle 2",
        _ => "unknown",
    }
}

fn topics_in(filter: &AffinityFilter) -> String {
    let present: Vec<&str> = [TOPIC_A, TOPIC_B, TOPIC_C]
        .iter()
        .filter(|topic| filter.contains(topic))
        .map(topic_name)
        .collect();
    if present.is_empty() {
        "{}".to_owned()
    } else {
        format!("{{{}}}", present.join(", "))
    }
}

/// One smoldot-light `Client`, as an application embeds it, with the relay
/// chain added once.
struct LightClient {
    client: Arc<Mutex<LightClientInner>>,
    relay: ChainId,
}

impl LightClient {
    fn new() -> Self {
        let mut client = Client::new(DefaultPlatform::new(
            env!("CARGO_PKG_NAME").into(),
            env!("CARGO_PKG_VERSION").into(),
        ));
        let relay = client
            .add_chain(AddChainConfig {
                user_data: (),
                specification: &relay_spec(),
                database_content: "",
                potential_relay_chains: iter::empty(),
                json_rpc: AddChainConfigJsonRpc::Disabled,
                statement_protocol_config: None,
            })
            .expect("add relay chain")
            .chain_id;
        LightClient {
            client: Arc::new(Mutex::new(client)),
            relay,
        }
    }

    /// One chain handle: an `add_chain` call for the parachain, which smoldot
    /// de-duplicates onto the running chain and gives its own JSON-RPC service.
    fn add_chain(&self, name: &'static str, specification: &str, bloom_seed: u128) -> ChainHandle {
        let success = self
            .client
            .lock()
            .unwrap()
            .add_chain(AddChainConfig {
                user_data: (),
                specification,
                database_content: "",
                potential_relay_chains: iter::once(self.relay),
                json_rpc: AddChainConfigJsonRpc::Enabled {
                    max_pending_requests: NonZero::new(128).unwrap(),
                    max_subscriptions: 1024,
                },
                statement_protocol_config: Some(StatementProtocolConfig::new(
                    NonZero::new(65_536).unwrap(),
                    0.01,
                    bloom_seed,
                    AFFINITY_UPDATE_INTERVAL,
                )),
            })
            .expect("add parachain");
        let inbox = Arc::new(Mutex::new(Vec::new()));
        let mut responses = success.json_rpc_responses.expect("JSON-RPC enabled");
        let sink = inbox.clone();
        thread::spawn(move || {
            while let Some(text) = smol::block_on(responses.next()) {
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    sink.lock().unwrap().push(value);
                }
            }
        });
        ChainHandle {
            name,
            client: self.client.clone(),
            chain_id: success.chain_id,
            inbox,
            next_id: Cell::new(1),
        }
    }
}

struct ChainHandle {
    name: &'static str,
    client: Arc<Mutex<LightClientInner>>,
    chain_id: ChainId,
    inbox: Arc<Mutex<Vec<Value>>>,
    next_id: Cell<u64>,
}

impl ChainHandle {
    fn call(&self, method: &str, params: Value) -> Value {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        self.client
            .lock()
            .unwrap()
            .json_rpc_request(
                json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string(),
                self.chain_id,
            )
            .expect("queue JSON-RPC request");
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some(answer) = self
                .inbox
                .lock()
                .unwrap()
                .iter()
                .find(|message| message["id"] == json!(id))
            {
                return answer
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| answer.clone());
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("{}: no answer to {method}", self.name);
    }

    fn subscribe(&self, topic: [u8; 32]) -> String {
        let filter = json!({ "matchAll": [format!("0x{}", hex::encode(topic))] });
        let id = self.call("statement_subscribeStatement", json!([filter]));
        id.as_str().expect("subscription id").to_owned()
    }

    fn unsubscribe(&self, subscription: &str) {
        let existed = self.call("statement_unsubscribeStatement", json!([subscription]));
        assert_eq!(existed, json!(true));
    }

    /// Labels of the statements delivered to this handle's subscriptions.
    fn delivered(&self) -> Vec<String> {
        let inbox = self.inbox.lock().unwrap();
        let mut labels = Vec::new();
        for message in inbox
            .iter()
            .filter(|message| message["method"] == "statement_statement")
        {
            let result = &message["params"]["result"];
            let statements = result["data"]["statements"]
                .as_array()
                .or_else(|| result["statements"].as_array());
            for encoded in statements.into_iter().flatten() {
                let bytes =
                    hex::decode(encoded.as_str().unwrap().trim_start_matches("0x")).unwrap();
                let statement = codec::decode_statement(&bytes).unwrap();
                labels.push(String::from_utf8(statement.data.unwrap_or_default()).unwrap());
            }
        }
        labels
    }

    /// Remove this chain handle, as an application does when its user goes away.
    fn remove(self) {
        let () = self.client.lock().unwrap().remove_chain(self.chain_id);
    }
}

struct Check {
    id: &'static str,
    expectation: String,
    observed: String,
    pass: bool,
}

struct Harness {
    peer: peer::Peer,
    parachain_spec: String,
    checks: Vec<Check>,
}

impl Harness {
    /// Start a fresh light client with chain handle 1 added, and wait until
    /// the peer has a statement substream with it.
    fn begin(&self, title: &str) -> (LightClient, ChainHandle, String) {
        println!("\n=== {title}");
        let before = self.peer.observed.lock().unwrap().statement_peers.len();
        let client = LightClient::new();
        let first = client.add_chain("handle 1", &self.parachain_spec, SEED_HANDLE_1);
        let peer_id = self.wait_statement_peer(before);
        (client, first, peer_id)
    }

    fn wait_statement_peer(&self, before: usize) -> String {
        let deadline = Instant::now() + Duration::from_secs(60);
        while Instant::now() < deadline {
            if let Some(peer) = self
                .peer
                .observed
                .lock()
                .unwrap()
                .statement_peers
                .get(before)
            {
                return peer.clone();
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("the light client never opened the statement protocol with the loopback peer");
    }

    fn filter_count(&self, from: &str) -> usize {
        self.peer
            .observed
            .lock()
            .unwrap()
            .filters
            .iter()
            .filter(|f| f.from == from)
            .count()
    }

    /// Wait for the next filter from `from` after `count` and print it.
    fn expect_filter(&self, from: &str, count: usize, after: &str) {
        let deadline = Instant::now() + FILTER_WAIT;
        while Instant::now() < deadline {
            if self.filter_count(from) > count {
                let latest = self.latest_filter(from).unwrap();
                println!(
                    "  {after}\n    -> peer received a filter built by {} containing {}",
                    author(&latest),
                    topics_in(&latest)
                );
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("no filter arrived after: {after}");
    }

    fn expect_no_filter(&self, from: &str, count: usize, after: &str) {
        thread::sleep(NO_FILTER_WAIT);
        let sent = self.filter_count(from) - count;
        println!("  {after}\n    -> filters received in the next {NO_FILTER_WAIT:?}: {sent}");
    }

    fn latest_filter(&self, from: &str) -> Option<AffinityFilter> {
        let observed = self.peer.observed.lock().unwrap();
        observed
            .filters
            .iter()
            .rev()
            .find(|f| f.from == from)
            .map(|f| f.filter.clone())
    }

    fn check_covers(&mut self, id: &'static str, from: &str, wanted: &[[u8; 32]]) {
        let latest = self.latest_filter(from).expect("a filter was received");
        let names: Vec<&str> = wanted.iter().map(topic_name).collect();
        let missing: Vec<&str> = wanted
            .iter()
            .filter(|topic| !latest.contains(topic))
            .map(topic_name)
            .collect();
        self.record(Check {
            id,
            expectation: format!(
                "the peer's current filter covers every open subscription: {{{}}}",
                names.join(", ")
            ),
            observed: format!(
                "filter built by {} containing {}{}",
                author(&latest),
                topics_in(&latest),
                if missing.is_empty() {
                    String::new()
                } else {
                    format!("; missing {missing:?}")
                }
            ),
            pass: missing.is_empty(),
        });
    }

    fn check_delivered(&mut self, id: &'static str, handle: &ChainHandle, label: &str) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline && !handle.delivered().iter().any(|l| l == label) {
            thread::sleep(Duration::from_millis(20));
        }
        let delivered = handle.delivered();
        self.record(Check {
            id,
            expectation: format!("{}'s open subscription receives {label}", handle.name),
            observed: format!("{} received {delivered:?}", handle.name),
            pass: delivered.iter().any(|l| l == label),
        });
    }

    fn send(&self, to: &str, statements: &[([u8; 32], &str)], forwarding: Forwarding) {
        let statements = statements
            .iter()
            .map(|(topic, label)| (*topic, (*label).to_owned()))
            .collect();
        for line in self.peer.send(to, statements, forwarding) {
            println!("    peer: {line}");
        }
    }

    fn record(&mut self, check: Check) {
        println!(
            "  [{}] {}: {}\n         observed: {}",
            if check.pass { "PASS" } else { "FAIL" },
            check.id,
            check.expectation,
            check.observed
        );
        self.checks.push(check);
    }
}

/// The relay chain spec from smoldot's repository, with its bootnodes removed
/// so that the light client contacts no public peer.
fn relay_spec() -> String {
    let text = std::fs::read_to_string(RELAY_SPEC_PATH).unwrap_or_else(|_| {
        panic!("{RELAY_SPEC_PATH} is missing: run ./fetch-relay-spec.sh first")
    });
    let mut spec: Value = serde_json::from_str(&text).expect("relay chain spec is JSON");
    spec["bootNodes"] = json!([]);
    spec.to_string()
}

fn parachain_genesis_hash() -> [u8; 32] {
    let client = LightClient::new();
    let probe = client.add_chain("genesis probe", PARACHAIN_SPEC, SEED_HANDLE_1);
    let hash = probe.call("chainSpec_v1_genesisHash", json!([]));
    hex::decode(hash.as_str().unwrap().trim_start_matches("0x"))
        .unwrap()
        .try_into()
        .unwrap()
}

fn main() {
    println!(
        "smoldot-light {SMOLDOT_LIGHT_VERSION}, offline: one loopback peer, no public bootnodes"
    );

    let genesis = parachain_genesis_hash();
    let peer = peer::spawn(genesis);
    let mut spec: Value = serde_json::from_str(PARACHAIN_SPEC).unwrap();
    spec["bootNodes"] = json!([peer.multiaddr.clone()]);
    let mut harness = Harness {
        peer,
        parachain_spec: spec.to_string(),
        checks: Vec::new(),
    };
    let spec = harness.parachain_spec.clone();

    {
        let (_client, only, peer_id) =
            harness.begin("C1 control: one chain handle subscribes to A, then B");
        let n = harness.filter_count(&peer_id);
        only.subscribe(TOPIC_A);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to A");
        let n = harness.filter_count(&peer_id);
        only.subscribe(TOPIC_B);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to B");
        harness.check_covers("C1", &peer_id, &[TOPIC_A, TOPIC_B]);
    }

    {
        let (client, first, peer_id) =
            harness.begin("C2 control: a second chain handle that never subscribes");
        let n = harness.filter_count(&peer_id);
        first.subscribe(TOPIC_A);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to A");
        let n = harness.filter_count(&peer_id);
        let second = client.add_chain("handle 2", &spec, SEED_HANDLE_2);
        second.call("chainSpec_v1_genesisHash", json!([]));
        harness.expect_no_filter(&peer_id, n, "handle 2 is added and makes a call");
        harness.check_covers("C2", &peer_id, &[TOPIC_A]);
    }

    {
        let (client, first, peer_id) =
            harness.begin("S1: two chain handles subscribe to different topics");
        let n = harness.filter_count(&peer_id);
        first.subscribe(TOPIC_A);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to A");
        let second = client.add_chain("handle 2", &spec, SEED_HANDLE_2);
        let n = harness.filter_count(&peer_id);
        second.subscribe(TOPIC_B);
        harness.expect_filter(&peer_id, n, "handle 2 subscribes to B");
        let substreams = harness
            .peer
            .observed
            .lock()
            .unwrap()
            .statement_peers
            .iter()
            .filter(|p| **p == peer_id)
            .count();
        println!("  statement substreams the peer has with this light client: {substreams}");
        harness.check_covers("S1", &peer_id, &[TOPIC_A, TOPIC_B]);

        println!("  peer ignores filters and sends a statement on A and one on B:");
        harness.send(
            &peer_id,
            &[(TOPIC_A, "A-1"), (TOPIC_B, "B-1")],
            Forwarding::IgnoreFilter,
        );
        harness.check_delivered("S1-control-delivery-1", &first, "A-1");
        harness.check_delivered("S1-control-delivery-2", &second, "B-1");

        println!("  peer honors the latest filter (polkadot-sdk rule) and offers A and B again:");
        harness.send(
            &peer_id,
            &[(TOPIC_A, "A-2"), (TOPIC_B, "B-2")],
            Forwarding::HonorLatestFilter,
        );
        harness.check_delivered("S1-delivery", &first, "A-2");
    }

    {
        let (client, first, peer_id) =
            harness.begin("S2: handle 2 unsubscribes; handle 1 is still subscribed");
        let n = harness.filter_count(&peer_id);
        first.subscribe(TOPIC_A);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to A");
        let second = client.add_chain("handle 2", &spec, SEED_HANDLE_2);
        let n = harness.filter_count(&peer_id);
        let subscription = second.subscribe(TOPIC_B);
        harness.expect_filter(&peer_id, n, "handle 2 subscribes to B");
        let n = harness.filter_count(&peer_id);
        second.unsubscribe(&subscription);
        harness.expect_filter(&peer_id, n, "handle 2 unsubscribes from B");
        harness.check_covers("S2", &peer_id, &[TOPIC_A]);
    }

    {
        let (client, first, peer_id) =
            harness.begin("S3: handle 2 is removed while it is still subscribed");
        let n = harness.filter_count(&peer_id);
        first.subscribe(TOPIC_A);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to A");
        let second = client.add_chain("handle 2", &spec, SEED_HANDLE_2);
        let n = harness.filter_count(&peer_id);
        second.subscribe(TOPIC_B);
        harness.expect_filter(&peer_id, n, "handle 2 subscribes to B");
        let n = harness.filter_count(&peer_id);
        second.remove();
        harness.expect_no_filter(&peer_id, n, "handle 2 is removed (remove_chain)");
        harness.check_covers("S3", &peer_id, &[TOPIC_A]);
        let n = harness.filter_count(&peer_id);
        first.subscribe(TOPIC_C);
        harness.expect_filter(&peer_id, n, "handle 1 subscribes to C");
        harness.check_covers("S3-recovery", &peer_id, &[TOPIC_A, TOPIC_C]);
    }

    let failed: Vec<&str> = harness
        .checks
        .iter()
        .filter(|check| !check.pass)
        .map(|check| check.id)
        .collect();
    println!(
        "\nsmoldot-light {SMOLDOT_LIGHT_VERSION}: {} checks, {} passed, {} failed {failed:?}",
        harness.checks.len(),
        harness.checks.len() - failed.len(),
        failed.len()
    );
    if std::env::var_os("REPRO_PEER_LOG").is_some() {
        println!("\nloopback peer log:");
        for line in &harness.peer.observed.lock().unwrap().log {
            println!("  {line}");
        }
    }
    if failed.is_empty() {
        println!("RESULT: not reproduced; every chain handle's subscriptions stay covered");
    } else {
        println!(
            "RESULT: reproduced; the advertised filter drops topics that another chain handle still subscribes to"
        );
        std::process::exit(1);
    }
}
