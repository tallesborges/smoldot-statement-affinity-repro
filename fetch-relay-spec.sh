#!/bin/sh
# Download the Paseo relay chain spec from smoldot's repository at the commit of
# the smoldot-light 3.0.0 release, and check it against a known SHA-256. The
# repro removes its bootnodes before use. It is not stored in this repository.
set -eu
commit=cd98d7fa6a69e0ea66b23c0a2fb6d36c9063c4bb
url="https://raw.githubusercontent.com/paritytech/smoldot/$commit/demo-chain-specs/paseo.json"
expected=72380e9c1515dbb35c30ecd4defe5689c4077a21c18eb8d19cf30dc72a328a00
out="$(dirname "$0")/specs/relay-paseo.json"

curl -fsSL "$url" -o "$out.tmp"
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$out.tmp" | cut -d' ' -f1)
else
  actual=$(shasum -a 256 "$out.tmp" | cut -d' ' -f1)
fi
if [ "$actual" != "$expected" ]; then
  rm -f "$out.tmp"
  echo "checksum mismatch for $url: got $actual" >&2
  exit 1
fi
mv "$out.tmp" "$out"
echo "wrote $out"
