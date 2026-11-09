---
id: walking-skeleton
title: "Walking skeleton: one real proxy segment to R2 and back through DataFusion"
status: todo
priority: p1
dependencies: [wal-frame-spec]
related: []
scopes: [app]
shared_scopes: [wal, store, query, project/tickets]
paths: []
tags: [archive, mvp]
---
## Goal

The thinnest end-to-end path that proves the stack before any of it is hardened: read one completed `.wal` segment, build a `turns` Arrow batch with every request stored whole in `request_raw`, write one Vortex file, upload it to the real R2 bucket with SSE-C, and run `SELECT count(*), min(ts), max(ts) FROM turns` against it through DataFusion.

## Why first

Three things can block the MVP and none of them is in our code: aligning `vortex`, `vortex-datafusion`, `datafusion`, `arrow` and `object_store` versions; R2 accepting SSE-C from `object_store`; and `vortex-datafusion` reading from an S3-compatible store with a custom endpoint. Finding out on day one is cheap; finding out after the splitter is written is not.

## Work

- `Cargo.toml` dependencies pinned to the versions in vortex-datafusion 0.93's lockfile.
- Minimal frame decoder sufficient for the vendored fixtures (the full reader with damage rules is `wal-reader`).
- Hard-coded Arrow schema for `turns` with the design record's columns, `request_raw` populated, `request_skeleton`/`request_parts` null, `blobs` not written.
- One `PUT` of the Vortex file to `turns/dt=<day>/<host>-seg-<seq>.vortex` with the SSE-C key; one `SELECT` through a `SessionContext` with a ListingTable over the prefix.
- Run it against a real segment copied from the proxy's spool and the real bucket. Record versions, gotchas and timings in `docs/skeleton-notes.md`.

## Acceptance

The query returns the fixture's row count from R2; a second run overwrites the same object and returns the same count; the notes file lists the exact crate versions that worked.
