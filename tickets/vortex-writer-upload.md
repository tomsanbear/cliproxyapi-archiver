---
id: vortex-writer-upload
title: Arrow schema, one Vortex file per WAL segment, and idempotent R2 upload with SSE-C
status: todo
priority: p1
dependencies: [walking-skeleton, wal-reader]
related: []
scopes: [store]
shared_scopes: [project/tickets]
paths: []
tags: [archive, vortex]
---
## Goal

Turn each decoded WAL segment into one `turns` and one `blobs` Vortex file uploaded to R2 with a customer-provided key, idempotently, so a crash at any point leaves the archive consistent.

## Work

- Arrow schema for `turns` and `blobs` as in the design record (fork/proposals/conversation-archive-2026-11-09.html in the proxy fork), with `schema_version`; `turns` includes `upstream_status`. Until the splitter lands, every request goes into `request_raw` and `blobs` files are empty.
- One `turns` and one `blobs` Vortex file per WAL segment, named `<host>-seg-<seq>.vortex` under `dt=<day of the segment's first ts>`. Replaying a segment overwrites the same two objects, so no manifest and no row dedupe are needed.
- Dedupe state (once the splitter exists): an in-memory set of blob hashes, rebuilt at start with `SELECT hash FROM blobs` through DataFusion. Nothing persisted.
- Writer: `vortex` crate with the edition pinned; take the `vortex`, `vortex-datafusion`, `datafusion`, `arrow` and `object_store` versions from vortex-datafusion's own lockfile so they align (the walking skeleton records the versions that worked).
- Upload: `object_store` S3 client against the R2 endpoint with `sse_customer_key_base64`; write the whole file locally first, then one `PUT` per object; confirm the WAL segment only after both objects are in R2.
- Config: spec section 8 (`archive.spool-dir` and `archive.r2.*` from the proxy's `config.yaml` via `--config`, or the equivalent flags/environment variables); fail fast on a missing bucket or key; never log secrets.

## Acceptance

Round trip test against `object_store`'s in-memory store (with the SSE-C header asserted on the S3 builder): write a segment's files, read them back with `vortex`, compare rows. Replaying the same segment leaves exactly two objects with identical content. Files open in DuckDB's vortex extension and in DataFusion.
