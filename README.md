# smoldot statement topic-affinity repro

## Bug

One smoldot-light `Client` adds the same parachain twice with `add_chain`. This
gives two chain handles. Each handle subscribes to a different statement topic.

The light client sends peers only the topic filter of the handle that changed
its subscriptions last. The other handle's topics are not in the filter. A peer
that applies the filter stops sending those statements, but their subscription
stays open.

Tested with smoldot-light 3.0.0 (latest release) and 2.1.0. Both show the bug.

## Run

```sh
cargo run --locked   # smoldot-light 3.0.0
cargo run --locked --no-default-features --features smoldot-light-2   # smoldot-light 2.1.0
```

Exit status `1` means the bug occurred. Exit status `0` means all checks passed,
which is the expected result after a fix.

## Expected and actual

Expected: the filter contains the open subscriptions of all handles of the
chain.

Actual (same for both versions):

| Steps | Filter the peer receives |
|---|---|
| Handle 1 subscribes to A. Handle 2 subscribes to B. | `{B}`. A is missing. |
| Then handle 2 unsubscribes from B. | `{}`. A is missing. |
| Then `remove_chain` removes handle 2 (not unsubscribed first). | No update. `{B}` stays. |

With one handle, the filter is correct. A peer that ignores the filter delivers
to both subscriptions, so only the filter is wrong.

## Cause

Each `add_chain` creates a JSON-RPC service, also for a chain that is already
running
([`lib.rs`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/lib.rs#L927-L928)).
Each service builds a filter from its own subscriptions
([`statement.rs`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/json_rpc_service/statement.rs#L367-L389)).
The shared network service keeps one filter per chain and replaces it with each
new filter
([`network_service.rs`](https://github.com/paritytech/smoldot/blob/cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb/light-base/src/network_service.rs#L2351-L2354)).

## Limits

- The peer is a test peer on 127.0.0.1, built from smoldot's `ChainNetwork`. It
  is not a full node. It records filters and can apply them in the same way as
  polkadot-sdk does for a light peer. The relay and parachain specs are
  synthetic, and the repro does not connect to a public network.
- Debug assertions are off. With them on, the light client panics during
  connection setup (`smoldot-5.0.0/src/libp2p/connection/yamux.rs:961`). This
  panic is not related to the filter. Release builds do not include these
  assertions.
- Tested only on macOS aarch64.

No license is chosen yet.
