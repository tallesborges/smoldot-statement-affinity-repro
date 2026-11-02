# smoldot statement topic-affinity repro

Tested against **smoldot-light 3.0.0**, the latest release on crates.io (same
commit as npm `smoldot` 3.7.0), and smoldot-light 2.1.0. Both reproduce.

## The problem

An application embeds one smoldot-light `Client` and calls `add_chain` more than
once with the same chain spec, for example once per independent user of the
chain. Each call returns its own chain handle (`ChainId`). smoldot de-duplicates
the chain: all handles share one network service, and each handle gets its own
JSON-RPC service.

Each JSON-RPC service builds its statement topic-affinity filter from its own
subscriptions only. The network service keeps one filter per chain and replaces
it with whichever filter arrived last. A peer that honors the filter then stops
sending statements that another handle is still subscribed to.

This repro runs one `Client`, adds the same parachain twice (handle 1 and
handle 2), subscribes each handle to different topics, and checks the filter a
loopback peer receives.

## Run

Needs a recent stable Rust (edition 2024), `curl` and a POSIX shell. Tested
with rustc 1.97.1 on macOS (aarch64).

```sh
./fetch-relay-spec.sh                                                # once; see "Chain specs"
cargo run --locked                                                   # smoldot-light 3.0.0
cargo run --locked --no-default-features --features smoldot-light-2  # smoldot-light 2.1.0
```

A cold build takes a few minutes. The run takes about 15 seconds. Set
`REPRO_PEER_LOG=1` to print the loopback peer's log.

**Exit status:** `1` means the bug reproduced (at least one check failed). `0`
means every check passed, which is the behavior expected after a fix.

## Result

| smoldot-light | smoldot (core) | Checks | Failed | Exit |
|---|---|---|---|---|
| 3.0.0 | 5.0.0 | 9 | S1, S1-delivery, S2, S3 | 1 |
| 2.1.0 | 4.0.0 | 9 | S1, S1-delivery, S2, S3 | 1 |

The output is the same on every run, because each handle uses a fixed bloom
seed. The 2.1.0 output differs only in the version number. smoldot-light 3.0.0:

```text
smoldot-light 3.0.0, offline: one loopback peer, no public bootnodes

=== C1 control: one chain handle subscribes to A, then B
  handle 1 subscribes to A
    -> peer received a filter built by handle 1 containing {A}
  handle 1 subscribes to B
    -> peer received a filter built by handle 1 containing {A, B}
  [PASS] C1: the peer's current filter covers every open subscription: {A, B}
         observed: filter built by handle 1 containing {A, B}

=== C2 control: a second chain handle that never subscribes
  handle 1 subscribes to A
    -> peer received a filter built by handle 1 containing {A}
  handle 2 is added and makes a call
    -> filters received in the next 3s: 0
  [PASS] C2: the peer's current filter covers every open subscription: {A}
         observed: filter built by handle 1 containing {A}

=== S1: two chain handles subscribe to different topics
  handle 1 subscribes to A
    -> peer received a filter built by handle 1 containing {A}
  handle 2 subscribes to B
    -> peer received a filter built by handle 2 containing {B}
  statement substreams the peer has with this light client: 1
  [FAIL] S1: the peer's current filter covers every open subscription: {A, B}
         observed: filter built by handle 2 containing {B}; missing ["A"]
  peer ignores filters and sends a statement on A and one on B:
    peer: A-1: sent
    peer: B-1: sent
  [PASS] S1-control-delivery-1: handle 1's open subscription receives A-1
         observed: handle 1 received ["A-1"]
  [PASS] S1-control-delivery-2: handle 2's open subscription receives B-1
         observed: handle 2 received ["B-1"]
  peer honors the latest filter (polkadot-sdk rule) and offers A and B again:
    peer: A-2: withheld, not in the client's latest filter
    peer: B-2: sent
  [FAIL] S1-delivery: handle 1's open subscription receives A-2
         observed: handle 1 received ["A-1"]

=== S2: handle 2 unsubscribes; handle 1 is still subscribed
  handle 1 subscribes to A
    -> peer received a filter built by handle 1 containing {A}
  handle 2 subscribes to B
    -> peer received a filter built by handle 2 containing {B}
  handle 2 unsubscribes from B
    -> peer received a filter built by handle 2 containing {}
  [FAIL] S2: the peer's current filter covers every open subscription: {A}
         observed: filter built by handle 2 containing {}; missing ["A"]

=== S3: handle 2 is removed while it is still subscribed
  handle 1 subscribes to A
    -> peer received a filter built by handle 1 containing {A}
  handle 2 subscribes to B
    -> peer received a filter built by handle 2 containing {B}
  handle 2 is removed (remove_chain)
    -> filters received in the next 3s: 0
  [FAIL] S3: the peer's current filter covers every open subscription: {A}
         observed: filter built by handle 2 containing {B}; missing ["A"]
  handle 1 subscribes to C
    -> peer received a filter built by handle 1 containing {A, C}
  [PASS] S3-recovery: the peer's current filter covers every open subscription: {A, C}
         observed: filter built by handle 1 containing {A, C}

smoldot-light 3.0.0: 9 checks, 5 passed, 4 failed ["S1", "S1-delivery", "S2", "S3"]
RESULT: reproduced; the advertised filter drops topics that another chain handle still subscribes to
```

What the checks show:

- **C1, C2 (controls):** one handle advertises a filter that covers all its
  subscriptions. A second handle that never subscribes changes nothing.
- **S1:** when both handles subscribe, the filter on the wire is handle 2's
  only. Both subscriptions stay open: a peer that ignores filters reaches both.
  A peer that honors the filter withholds handle 1's topic.
- **S2:** when handle 2 unsubscribes from its last topic, it sends an empty
  filter. Handle 1's open subscription then has no coverage.
- **S3:** removing handle 2 with `remove_chain` sends no update. Handle 2's
  topics stay advertised and handle 1's do not. Coverage comes back only when
  handle 1 changes its own subscriptions (S3-recovery).

## Expected behavior

The filter a peer holds for a chain covers the open subscriptions of every
handle of that chain on the `Client`. It is updated when a handle unsubscribes
or is removed.

smoldot documents that calling `addChain` more than once with the same chain
spec is supported, and encourages it when one client serves several purposes
([`addChain` docs](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/wasm-node/javascript/src/public-types.ts#L91-L96)).

## Where it happens (smoldot-light 3.0.0, commit `cd98d7fa6`)

- A new JSON-RPC service for every `add_chain`, even for a chain already
  running:
  [`light-base/src/lib.rs#L927-L928`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/lib.rs#L927-L928)
- Each JSON-RPC service builds its filter from its own subscriptions:
  [`json_rpc_service/statement.rs#L367-L389`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/json_rpc_service/statement.rs#L367-L389),
  and sends it to the shared network service:
  [`json_rpc_service/background.rs#L922-L928`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/json_rpc_service/background.rs#L922-L928)
- The network service replaces the chain's filter:
  [`network_service.rs#L2351-L2354`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/network_service.rs#L2351-L2354)

The same code is in smoldot-light 2.1.0.

## How it works

- **Light client:** the published `smoldot-light` crate, used only through its
  public API (`Client::new`, `add_chain`, `json_rpc_request`, `remove_chain` and
  `JsonRpcResponses`) on `DefaultPlatform`. Each scenario uses a fresh `Client`
  with the relay chain added once, then adds the parachain once per handle.
- **Loopback peer (`src/peer.rs`):** built from the `smoldot` crate's
  `ChainNetwork`, the networking state machine smoldot-light uses. It listens on
  127.0.0.1, accepts the light client's connection, records each topic-affinity
  filter, and sends statements on request. It is not a full node: it has no
  statement store and does not verify signatures. Its "honors the latest
  filter" mode applies the rule polkadot-sdk's statement protocol applies to a
  light peer: replace the peer's filter on each update
  ([`lib.rs#L2005`](https://github.com/paritytech/polkadot-sdk/blob/7a8daa68e18718c18823c1109513fdc1c8aa71fe/substrate/client/network/statement/src/lib.rs#L2005))
  and skip statements it does not match
  ([`lib.rs#L883`](https://github.com/paritytech/polkadot-sdk/blob/7a8daa68e18718c18823c1109513fdc1c8aa71fe/substrate/client/network/statement/src/lib.rs#L883)).
- **Topics and seeds:** topics A, B and C are synthetic 32-byte values. Each
  handle gets a fixed bloom seed, so the seed in a received filter shows which
  handle's JSON-RPC service built it. None of the checked topics hit a bloom
  false positive with these seeds.
- **Statements:** each carries a dummy Ed25519 proof and a future expiry. This
  is all smoldot-light checks before delivery; it does not verify signatures.

## Chain specs

- **Relay chain:** `fetch-relay-spec.sh` downloads `demo-chain-specs/paseo.json`
  from smoldot's repository at commit `cd98d7fa6` (the smoldot-light 3.0.0
  release) and checks its SHA-256. The file is not stored in this repository.
  The repro removes its bootnodes before use, so the light client contacts no
  public peer. Nothing syncs; the statement methods do not need it.
- **Parachain:** `specs/parachain.json` is synthetic (id
  `statement-affinity-repro`, para id 4242). Its only bootnode is set at run
  time to the loopback peer.

## Harness workarounds

- **Placeholder expected peer id.** `ChainNetwork` indexes a connection by peer
  only when it was added with an expected peer id, and `gossip_open` needs that
  index. The loopback peer passes a placeholder for each inbound connection;
  the handshake replaces it with the real id.
- **`debug-assertions = false` in the dev profile.** With debug assertions on,
  the light client side panics during connection setup at
  `smoldot-5.0.0/src/libp2p/connection/yamux.rs:961`
  (`debug_assert!(_was_inserted)` on an incoming RST). Release builds, which is
  how smoldot ships, compile these assertions out. This assertion is unrelated
  to the affinity behavior. Whether a real full node would trigger it too was
  not investigated.

## Scope and limits

- Offline at run time: the only network traffic is between the light client
  and the loopback peer on 127.0.0.1. Nothing is submitted to a public network.
  The build downloads crates, and `fetch-relay-spec.sh` downloads one file from
  GitHub.
- Proves what the light client advertises, and models what a filter-honoring
  peer then sends. It does not run a polkadot-sdk node or measure delivery loss
  on a live network.
- Tested on macOS aarch64 only.

## License

No license has been chosen for this repository yet. Its dependencies, including
smoldot and smoldot-light (GPL-3.0-or-later WITH Classpath-exception-2.0), keep
their own licenses.
