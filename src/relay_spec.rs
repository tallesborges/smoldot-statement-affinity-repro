//! A synthetic relay chain spec, built here so the repro needs no external
//! file. smoldot-light needs a relay chain to add a parachain. A relay chain
//! without a runtime in its genesis needs a checkpoint (`lightSyncState`), so
//! this builds the smallest checkpoint smoldot accepts: one BABE and GRANDPA
//! authority, two BABE epochs and a finalized block 1. The spec has no
//! bootnodes, and nothing is synced.

use serde_json::json;

const AUTHORITY: [u8; 32] = [0x0A; 32];
const SLOTS_PER_EPOCH: u64 = 10;

pub fn json() -> String {
    json!({
        "name": "Statement Affinity Repro Relay",
        // `specs/parachain.json` names this id as its `relay_chain`.
        "id": "statement-affinity-repro-relay",
        "chainType": "Live",
        "bootNodes": [],
        "telemetryEndpoints": null,
        "protocolId": null,
        "properties": {},
        "codeSubstitutes": {},
        "genesis": { "stateRootHash": hex(&[0x22; 32]) },
        "lightSyncState": {
            "babeEpochChanges": hex(&babe_epoch_changes()),
            "babeFinalizedBlockWeight": 1,
            "finalizedBlockHeader": hex(&finalized_header()),
            "grandpaAuthoritySet": hex(&grandpa_authority_set()),
        },
    })
    .to_string()
}

fn hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// SCALE compact encoding of a value below 64.
fn compact(value: u8) -> u8 {
    assert!(value < 64);
    value << 2
}

/// SCALE compact encoding of 64, the length of a seal.
const COMPACT_64: [u8; 2] = [0x01, 0x01];

/// Block 1, in slot 15 (epoch 1), with a BABE pre-runtime digest and a seal.
fn finalized_header() -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&[0x33; 32]); // parent hash
    header.push(compact(1)); // block number
    header.extend_from_slice(&[0x44; 32]); // state root
    header.extend_from_slice(&[0x55; 32]); // extrinsics root
    header.push(compact(2)); // digest items

    let mut pre_digest = vec![2]; // BABE secondary plain pre-digest
    pre_digest.extend_from_slice(&0u32.to_le_bytes()); // authority index
    pre_digest.extend_from_slice(&15u64.to_le_bytes()); // slot
    header.push(6); // pre-runtime digest item
    header.extend_from_slice(b"BABE");
    header.push(compact(pre_digest.len() as u8));
    header.extend_from_slice(&pre_digest);

    header.push(5); // seal digest item
    header.extend_from_slice(b"BABE");
    header.extend_from_slice(&COMPACT_64);
    header.extend_from_slice(&[0; 64]);
    header
}

/// An empty fork tree, then epoch 1 (slots 10..20) and epoch 2 (slots 20..30).
fn babe_epoch_changes() -> Vec<u8> {
    let mut out = vec![compact(0), 0]; // no roots, no best finalized number
    out.push(compact(2));
    // smoldot keys epochs by (block hash, block number), so each needs its own.
    for (index, start_slot, block_number) in [(1u64, 10u64, 0u32), (2, 20, 1)] {
        out.extend_from_slice(&[0x33 + block_number as u8; 32]); // block hash
        out.extend_from_slice(&block_number.to_le_bytes());
        out.push(1); // regular epoch
        out.extend_from_slice(&index.to_le_bytes());
        out.extend_from_slice(&start_slot.to_le_bytes());
        out.extend_from_slice(&SLOTS_PER_EPOCH.to_le_bytes());
        out.push(compact(1));
        out.extend_from_slice(&AUTHORITY);
        out.extend_from_slice(&1u64.to_le_bytes()); // weight
        out.extend_from_slice(&[0x66; 32]); // randomness
        out.extend_from_slice(&1u64.to_le_bytes()); // c numerator
        out.extend_from_slice(&4u64.to_le_bytes()); // c denominator
        out.push(1); // primary and secondary plain slots
    }
    out
}

/// One authority in set 0, with no pending changes.
fn grandpa_authority_set() -> Vec<u8> {
    let mut out = vec![compact(1)];
    out.extend_from_slice(&AUTHORITY);
    out.extend_from_slice(&1u64.to_le_bytes()); // weight
    out.extend_from_slice(&0u64.to_le_bytes()); // set id
    out.extend_from_slice(&[compact(0), 0]); // no pending standard changes
    out.push(compact(0)); // no pending forced changes
    out.push(compact(0)); // no authority set changes
    out
}
