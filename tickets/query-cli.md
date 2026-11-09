---
id: query-cli
title: DataFusion query CLI over the R2 archive with a reassemble UDF
status: todo
priority: p2
dependencies: [vortex-writer-upload]
related: []
scopes: [query]
shared_scopes: [project/tickets]
paths: []
tags: [archive, datafusion]
---
## Goal

`cliproxyapi-archiver query '<sql>'` runs DataFusion SQL over `turns` and `blobs` on R2 and can return any turn's original request bytes.

## Work

- `SessionContext` with `vortex-datafusion` ListingTables for `turns/` and `blobs/` (partition column `dt`), S3 object store carrying the SSE-C key, a local on-disk cache of fetched files.
- Scalar UDF `reassemble(skeleton, parts)` that joins hashes to `blobs` and returns the original body; a table function or view `request_body(request_id)` for convenience.
- Output formats: table, JSON lines, CSV; `--explain`.
- Saved queries shipped as SQL files: turns per session, tokens and cache hit rate per account per day, cost per day, sessions by client version.

## Acceptance

Querying a day's archive returns within seconds for metadata-only queries; `reassemble` on a sampled turn equals the bytes recorded by the proxy for that request id.
