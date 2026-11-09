---
id: wal-reader
title: "WAL reader: discover segments, decode frames per the spec's damage rules, drain oldest-first"
status: todo
priority: p1
dependencies: [walking-skeleton]
related: []
scopes: [wal]
shared_scopes: [project/tickets]
paths: []
tags: [archive]
---
## Goal

A reader that turns the proxy's spool directory into a stream of decoded turn records, in the order the spec requires, and deletes a segment only after the caller confirms its rows are durable.

## Work

- List `seg-*.wal` (completed segments only; never read, delete or rename `.wal.open`), parse the 12-digit sequence numbers, drain in ascending order. Expose the ordering as a pure function over the listing for testing.
- Decode per spec sections 5 and 6: 20-byte little-endian header (`CPAW`, version, flags, payload_len, record_len, crc32c), per-frame zstd payload, three length-prefixed record sections. Damage rules exactly as specified: truncated tail ends quietly; bad magic or `payload_len` over limit ends as corrupt; unknown version, CRC mismatch and bad record are skipped and counted. Readers ignore unknown flag bits.
- Segment lifecycle: yield `(segment, Vec<Record>)` plus the read stats; `confirm(segment)` unlinks it; `ENOENT` on unlink is success.
- Keep the read stats for the `status` subcommand (ticket `launchd-deploy`).

## Acceptance

Tests over the vendored fixtures reproduce `manifest.json` exactly (counts and section hashes for every `.wal`, `.wal.open` ignored); a segment deleted by the proxy mid-run is tolerated; a crash between upload and confirm re-reads the segment and the writer stage's per-segment idempotent upload makes the replay harmless.
