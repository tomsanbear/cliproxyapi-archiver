# Walking skeleton notes

What the `walking-skeleton` ticket proved, the crate versions that worked, and how to run it. The binary is `skeleton` (`src/main.rs`); it is deliberately the thinnest path and has no daemon loop, splitter, `blobs` table or ring logic.

## Crate versions that compiled and ran

Toolchain: Rust 1.95.0 (stable; vortex 0.93 declares `rust-version = 1.95`).

| Crate | Version | Requirement in `Cargo.toml` | Source of the version |
|---|---|---|---|
| `vortex` | 0.93.0 | `=0.93.0`, features `object_store`, `tokio` | crates.io |
| `vortex-datafusion` | 0.93.0 | `=0.93.0` | crates.io (published 2026-11-06, no git dependency needed) |
| `datafusion` | 55.2.0 | `=55.2.0`, `default-features = false` plus `sql` and the expression features | vortex 0.93.0 `Cargo.lock` |
| `arrow` | 59.3.2 | `=59.3.2`, features `chrono-tz`, `prettyprint` | vortex 0.93.0 `Cargo.lock` |
| `object_store` | 0.13.2 | `=0.13.2`, features `aws`, `fs` | vortex 0.93.0 `Cargo.lock` |
| `tokio` | 1.54.1 | `1.54.1` | vortex 0.93.0 `Cargo.lock` |
| `zstd` | 0.14.3 | `0.14.3` | vortex 0.93.0 `Cargo.lock` (`vortex-zstd` uses the same) |
| `crc32c` | 0.6.9 | `0.6` | crates.io (the only crate not in vortex's lockfile) |
| `serde` / `serde_json` | 1.0.229 / 1.0.154 | | vortex 0.93.0 `Cargo.lock` |
| `clap` | 4.6.7 | features `derive`, `env` | vortex 0.93.0 `Cargo.lock` |
| `chrono` | 0.4.45 | | vortex 0.93.0 `Cargo.lock` |
| `anyhow`, `base64`, `url` | 1.0.104, 0.22.1, 2.5.8 | | vortex 0.93.0 `Cargo.lock` |
| `sha2`, `tempfile` (dev) | 0.11.1, 3.27.0 | | vortex 0.93.0 `Cargo.lock` |

`Cargo.lock` was seeded from vortex's own lockfile at tag `0.93.0` (`git show 0.93.0:Cargo.lock`) and then resolved by Cargo, so every third-party crate in the graph is a version vortex 0.93.0 itself is tested with; the only additions are the `vortex-*` crates from crates.io and `crc32c`. `datafusion`, `arrow` and `object_store` each resolve to a single version, so there are no duplicate Arrow or DataFusion stacks.

To bump: take the new vortex tag's lockfile, update the four `=` pins together, reseed `Cargo.lock` the same way, and rerun the tests.

Incompatibilities hit: none. Everything compiled at the first attempt once the versions were taken from the lockfile. Note that vortex's lockfile also contains datafusion 54.1.0 / arrow 58.4.1 / object_store 0.14.2, pulled in by the Lance comparison benchmarks (`lance` 13) and `object_store_opendal`; the ones `vortex-datafusion` 0.93.0 links against are 55.2.0 / 59.3.2 / 0.13.2.

## API shapes

Write path (`src/store.rs`):

- Session: `VortexSession::default().with_tokio()` (`vortex::VortexSessionDefault`, `vortex::io::session::RuntimeSessionExt`). `with_tokio` captures the current Tokio runtime, so the session must be created inside it.
- Edition pin: `session.enable_edition(vortex::editions::CORE_2026_08_3)` (`EditionSessionExt`). At most one edition per family is enabled, and enabling one replaces the family's previous selection. `CORE_2026_08_3` is also the 0.93 default; pinning it explicitly means a later bump that moves the default does not silently change which encodings readers must understand.
- Arrow to Vortex: `vortex::arrow::convert::from_arrow_batch(&batch, false)` gives a struct `ArrayRef`; Timestamp with timezone, `List<Binary>` and all-null columns convert without help.
- Write: `session.write_options().write(&mut Vec<u8>, array.to_array_stream()).await` returns a `WriteSummary` with the size. `Vec<u8>`, `tokio::fs::File` and vortex's `ObjectStoreWrite` all implement `VortexWrite`; the skeleton writes to memory and does one `PUT`.

Read path (`src/query.rs`):

- `ListingOptions::new(Arc::new(VortexFormat::new(session))).with_file_extension(".vortex").with_table_partition_cols(vec![("dt", Utf8)])`, then `ListingTableConfig::new(url).with_listing_options(..).infer_schema(&ctx.state())` and `ListingTable::try_new`. This is the same shape as `vortex-datafusion/examples/vortex_table.rs`.
- The archive's object store is registered with `ctx.register_object_store(url, store)`; vortex-datafusion reads through whatever the DataFusion runtime resolves for the table URL, so a custom scheme works.

## Surprises and gotchas

- **Types come back as views.** Columns written as Arrow `Utf8` and `Binary` are inferred back as `Utf8View` and `BinaryView` (`List<Binary>` as `List<BinaryView>`); integers, booleans and `Timestamp(ns, "UTC")` round-trip exactly. Readers must not assume the write schema; the schema-evolution plan (merge on read, new columns nullable) is unaffected.
- **Fixed per-file overhead is large.** Three fixture turns (3.8 KB of WAL, about 7 KB of raw bodies) produce a 34.8 KB Vortex file. At real segment sizes this is noise, but small or idle segments will be dominated by footer and layout metadata; worth measuring on a real 15-minute segment.
- **`chrono-tz` is required for a named timezone.** Without arrow's `chrono-tz` feature, `"UTC"` cannot be cast or displayed (only `+00:00` offsets are). DataFusion enables it anyway; the dependency is declared explicitly so the schema does not rely on feature unification.
- **Local stores get their own URL scheme.** The offline mode uses `LocalFileSystem::new_with_prefix(--out)` registered as `archive-local://root/`, so object keys are identical to the R2 layout and the rooted store does not shadow `file://` in the DataFusion runtime.
- **`AmazonS3Builder::new()`, not `from_env()`.** Only `ARCHIVE_R2_*` settings apply; ambient `AWS_*` variables are ignored. Path-style requests, region `auto`.
- **object_store does not validate the SSE-C key length.** It only base64-decodes it to compute the MD5 header, so the skeleton checks for exactly 32 bytes and fails before any request.
- **`cargo test` is blocked in this environment** in favour of `cargo nextest run`; both run the same tests.

## Timings

Offline, release build, Apple Silicon, fixture `seg-000000000001.wal` (3 records), second run with warm caches: decode 0.1 ms, build 0.1 ms, Vortex write 5.7 ms, local put plus head 0.3 ms. First run in a process: write 25.7 ms (session and compressor warm-up). `SELECT count(*), min(ts), max(ts) FROM turns` took about 100 ms cold including session setup and schema inference; later queries 4–20 ms.

Build: a clean debug build of the dependency graph takes about 2 minutes, a clean release build about 4.5 minutes.

R2 timings: not measured yet; see below.

## Running it

Offline (the test suite does this in a temp directory):

```sh
cargo nextest run          # or: cargo test
cargo run --release --bin skeleton -- ingest --spool fixtures/wal --segment 1 --out /tmp/archive
cargo run --release --bin skeleton -- query --out /tmp/archive 'SELECT count(*), min(ts), max(ts) FROM turns'
```

`tests::ingest_then_query_offline_and_replay_overwrites` ingests segment 1 twice and checks the count is 3 both times and exactly one object exists; `tests::bodies_and_metadata_round_trip_through_vortex_and_datafusion` checks `request_raw` and `response_raw` come back byte-identical to the WAL sections.

Against R2 (not yet run; no credentials existed when the skeleton was written):

```sh
export ARCHIVE_R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
export ARCHIVE_R2_BUCKET=agent-archive
export ARCHIVE_R2_PREFIX=skeleton            # optional; keeps the test object away from real data
export ARCHIVE_R2_ACCESS_KEY_ID=...
export ARCHIVE_R2_SECRET_ACCESS_KEY=...
export ARCHIVE_R2_SSE_C_KEY="$(openssl rand -base64 32)"   # store it: objects are unreadable without it

# Copy a completed segment out of the proxy's spool first; the skeleton never deletes, but it
# should not read the live spool either.
mkdir -p /tmp/skeleton-spool && cp ~/.local/share/cliproxyapi/archive-spool/seg-<seq>.wal /tmp/skeleton-spool/
cargo run --release --bin skeleton -- ingest --spool /tmp/skeleton-spool --segment <seq>
cargo run --release --bin skeleton -- query 'SELECT count(*), min(ts), max(ts) FROM turns'
# Acceptance: run ingest again; the query returns the same count and the bucket holds one object.
```

Every R2 setting can also be passed as a flag (`--r2-endpoint`, `--r2-bucket`, `--r2-prefix`, `--r2-access-key-id`, `--r2-secret-access-key`, `--r2-sse-c-key`); secrets are hidden from `--help` and never printed. Ingest does one `PUT` and then a `HEAD` with the same SSE-C headers to confirm the object is readable, so a successful ingest already proves R2 accepts SSE-C on write and read. The query then proves vortex-datafusion's ranged reads work against the R2 endpoint with SSE-C.

Still open until the R2 run: whether R2 accepts object_store 0.13's SSE-C headers on `PUT`, `HEAD` and ranged `GET`, and the upload and query timings over the network.
