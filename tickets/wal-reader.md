---
id: wal-reader
title: "WAL reader: discover segments, decode frames, drain with ring awareness"
status: todo
priority: p1
dependencies: [wal-frame-spec]
related: []
scopes: [wal]
shared_scopes: [project/tickets]
paths: []
tags: [archive]
---
## Goal

A reader that turns the proxy's spool directory into a stream of decoded turn records, in the order the spec requires, and deletes a segment only after the caller confirms its rows are durable.

## Work

- List `seg-*.zst`, parse sequence numbers, treat the highest as in-progress (never read it).
- Drain oldest-first; when more than the high-water count (12) of completed segments exist, drain newest-first until back under it. Expose the policy as a pure function over the listing for testing.
- Decode: zstd stream → `u32 length` + `u8 version` + payload frames; skip a truncated tail; read an unterminated stream to the last complete block; count and skip unknown versions.
- Segment lifecycle: yield `(segment, Vec<Turn>)`; `confirm(segment)` unlinks it; `ENOENT` on unlink is success.
- Write `archiver-status.json` into the spool dir after each confirm: last segment, frames read, last upload time, errors. The proxy's management panel shows it.

## Acceptance

Tests over the vendored fixtures and over synthetic spools: ordering under and over high-water; a segment deleted by the proxy mid-run is tolerated; a crash between upload and confirm re-reads the segment and the writer stage dedupes by `request_id`+`attempt`.
