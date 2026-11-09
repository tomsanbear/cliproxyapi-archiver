---
id: vortex-writer-upload
title: Arrow schema, Vortex writer with hourly roll, and R2 upload with SSE-C
status: todo
priority: p1
dependencies: [wal-reader, structural-splitter]
related: []
scopes: [store]
shared_scopes: [project/tickets]
paths: []
tags: [archive, vortex]
---
## Goal

Turn decoded turns into `turns` and `blobs` Vortex files partitioned by hour and uploaded to R2 with a customer-provided key, idempotently, so a crash at any point leaves the archive consistent.

## Work

- Arrow schema for `turns` and `blobs` as in the design record (fork/proposals/conversation-archive-2026-11-09.html in the proxy fork), with `schema_version`.
- Dedupe: an in-process set of blob hashes seen in the current hour plus a persisted bloom/exact index of hashes already uploaded (small; rebuildable from R2 by listing `blobs/`). A blob is written in the same hourly batch as the first turn that references it.
- Writer: `vortex` crate, pin the edition; one `turns` and one `blobs` file per hour of row timestamps (`ts`), file names carry hour and a sequence so late segments add a file rather than rewrite one.
- Upload: `object_store` S3 client against the R2 endpoint with `sse_customer_key_base64`; `PUT` to a temporary key then rename/copy to final, or write final key only after the whole file is staged locally; never leave a partial final object.
- Config: read the proxy's `config.yaml` `archive` section (path given by `--config`); fail fast on a missing bucket or key; never log secrets.
- Confirm WAL segments only after both files of the batch are in R2.

## Acceptance

Round trip test against a local S3-compatible server (MinIO or `object_store`'s in-memory store with the SSE-C header asserted): write a batch, read it back with `vortex`, compare rows. Crash injection between upload and confirm produces no duplicate rows after replay. Files open in DuckDB's vortex extension and in DataFusion.
