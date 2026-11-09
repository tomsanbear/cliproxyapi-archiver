# Archive WAL format

Version 1. This document is the contract between the CLIProxyAPI archive writer (Go, `internal/archive`) and `cliproxyapi-archiver` (Rust). Both repositories carry an identical copy of this file and of the golden fixtures under `internal/archive/testdata/wal/` (vendored as `fixtures/wal/` in the archiver). A change to the format changes this document first, in both places, with a version bump.

The WAL exists so that the proxy can record every conversation turn with one local file append and nothing else. It is a transient buffer, not the archive: the archiver drains it into Vortex files on R2 and deletes what it has stored.

## 1. Spool directory

One directory, `archive.spool-dir` in the proxy config. It contains:

| Entry | Written by | Meaning |
|---|---|---|
| `seg-<seq>.wal` | proxy | A completed segment. Immutable once it has this name. |
| `seg-<seq>.wal.open` | proxy | The segment being written. At most one exists while the proxy runs. |
| `spool.lock` | proxy | Advisory lock (`flock`, exclusive, non-blocking) held for the writer's lifetime. |

The archiver may keep its own state files in the directory under names starting with `archiver-`; the writer ignores them. Their contents are not part of this contract.

`<seq>` is a decimal `u64`, zero-padded to 12 digits, strictly increasing by one per segment: `seg-000000000001.wal`. Names sort lexicographically in sequence order. The directory listing is the only index; there is no catalogue file.

There is exactly one writer and one reader per spool directory. The writer takes `spool.lock` at start and refuses to run the archive if it cannot: a second proxy instance pointed at the same spool (for example a deploy preflight started from a copied config) logs an error and disables archiving rather than interleaving writes. The reader never takes the lock.

## 2. Segment lifecycle

1. The writer creates `seg-<seq>.wal.open` when it has the first frame to write, with `seq` one greater than the highest sequence present in the directory (counting both `.wal` and `.wal.open`), or 1 for an empty directory. An idle proxy creates no segments.
2. Frames are appended to the open segment. The writer may buffer in memory, but every frame is written whole: a frame's bytes are never split across a flush boundary.
3. The segment rolls when its size reaches `archive.segment-max-bytes` (default 64 MiB) or when `archive.segment-max-age` (default 15 minutes) has passed since its first frame. Rolling means: flush, `fsync` the file, rename `seg-<seq>.wal.open` to `seg-<seq>.wal`, `fsync` the directory.
4. A clean shutdown rolls the open segment the same way.
5. At start, if a `seg-<seq>.wal.open` exists (the previous process crashed), the writer rolls it before creating a new segment: `fsync` the file, rename, `fsync` the directory, exactly as in step 3. Its tail may hold a truncated frame; see §6.

The reader considers only `seg-<seq>.wal` files. It never reads, deletes or renames a `.wal.open` file.

## 3. Ring

Completed segments form a ring of at most `archive.ring-segments` (default 16, so 1 GiB at the default segment size). When a roll would leave more than that many completed segments, the writer unlinks the lowest-numbered completed segment first, and increments its `segments_dropped` counter and the frame count that was lost.

Dropping the oldest is deliberate: a later turn of the same session carries the earlier turns' messages in its request body, so the oldest unarchived frames are the cheapest to lose. The newest are never dropped.

Unlinking a file the reader has open is safe on the supported platforms (macOS, Linux): the reader finishes from its descriptor. Both sides treat `ENOENT` on unlink as success.

## 4. Reader rules

- Drain completed segments in ascending sequence order.
- Process a whole segment, make its rows durable (uploaded and confirmed), then unlink it. A crash between upload and unlink causes the segment to be processed again, so the archive output for a segment must be idempotent: derive the output object names from the segment sequence (and the writer host), so a replay overwrites the same objects instead of adding rows.
- `(request_id, attempt)` is unique across the whole WAL and may be used as a row key downstream.
- Do not rely on processing order for archive layout: rows carry their own `ts`.

## 5. Frame

A segment is a sequence of frames with no file header and no padding. Every frame is a 20-byte header followed by a payload. Integers are little-endian.

| Offset | Size | Field | Value |
|---|---|---|---|
| 0 | 4 | `magic` | ASCII `CPAW` |
| 4 | 1 | `version` | `1` |
| 5 | 1 | `flags` | bit 0: payload compression, `0` = none, `1` = zstd. Other bits `0`. |
| 6 | 2 | `reserved` | `0` |
| 8 | 4 | `payload_len` | byte length of the payload that follows the header |
| 12 | 4 | `record_len` | byte length of the record after decompression (equal to `payload_len` when uncompressed) |
| 16 | 4 | `crc32c` | CRC-32C (Castagnoli) of the payload bytes as stored |
| 20 | `payload_len` | `payload` | the record (§6), compressed per `flags` |

The header is 20 bytes in every version, and bytes 0–11 (`magic`, `version`, `flags`, `reserved`, `payload_len`) keep their meaning in every version, so a reader can always skip a frame whose version it does not understand. Readers ignore `flags` bits they do not know and treat `reserved` as opaque.

Version 1 writers always compress with zstd (`flags` bit 0 = 1), one independent zstd frame per WAL frame, with the zstd content checksum enabled. Per-frame compression costs some ratio against a single stream but means a truncated or corrupt frame never affects its neighbours, and a reader can decode any frame without state.

Limits: `payload_len` ≤ 128 MiB, `record_len` ≤ 256 MiB. Writers never produce frames beyond these. A reader treats a `payload_len` over the limit as corruption of the header (§6 step 2, since it can no longer trust the length to skip by) and a `record_len` over the limit as a bad record (§6 step 6).

## 6. Reading a segment, including damage

The reader walks frames from offset 0:

1. Fewer than 20 bytes remain: the segment ends. If more than 0 bytes remain this is a **truncated tail**; count it and stop. This is the normal result of a crash mid-write and is not an error.
2. `magic` mismatch, or `payload_len` over its limit: the segment is **corrupt** from this point. Count it and stop; frames read so far are valid. (Bytes before a valid header are never scanned for a later magic.)
3. `version` unknown: skip `payload_len` bytes, count `unknown_version`, continue.
4. Fewer than `payload_len` bytes remain: truncated tail; count and stop.
5. `crc32c` mismatch: skip the payload, count `crc_mismatch`, continue.
6. `record_len` over its limit, decompression failure, or a decompressed length that differs from `record_len`: skip, count `bad_record`, continue.
7. Unpack the record (§7); a record that does not unpack is counted as `bad_record` and skipped.

Readers report these counts; the archiver surfaces them in `archiver-status.json`. Tests compare **decoded** records, never compressed bytes, because zstd output differs between encoders and versions.

## 7. Record

The decompressed payload is three length-prefixed sections:

| Field | Size | Content |
|---|---|---|
| `meta_len` | u32 LE | |
| `meta` | `meta_len` | UTF-8 JSON object (§7.1) |
| `request_len` | u32 LE | |
| `request` | `request_len` | inbound request body, exact bytes after transport decoding |
| `response_len` | u32 LE | |
| `response` | `response_len` | response bytes exactly as delivered to the client, before transport encoding |

`record_len` = 12 + `meta_len` + `request_len` + `response_len`.

"After transport decoding" means: if the client sent the body with `Content-Encoding: zstd` or `gzip`, the stored request is the decoded body; if the proxy compressed the response to the client, the stored response is the bytes before compression. The response section is every byte the proxy wrote to the client's response body for this request, in order: for streams that is the SSE text including keep-alive comment lines (`: keep-alive`) and any `event: error` frame the proxy synthesized; for errors it is the error JSON. The writer captures it with one response-writer wrapper, so no write path is missed and a client-side capture can be compared byte-for-byte.

### 7.1 Meta

One JSON object per record. Keys not listed here may appear in later versions; readers ignore unknown keys. Timestamps are RFC 3339 in UTC with nanosecond precision (`2026-11-09T18:04:11.123456789Z`). Strings are empty rather than `null` when unknown; integers are `0` when unknown unless stated otherwise.

```json
{
  "v": 1,
  "request_id": "a1b2c3d4-…",
  "attempt": 1,
  "ts": "2026-11-09T18:04:11.123456789Z",
  "completed_at": "2026-11-09T18:04:13.001000000Z",
  "host": "mbp.local",
  "proxy_version": "v8.0.51-local",
  "client": {
    "method": "POST",
    "path": "/v1/messages",
    "query": "beta=true",
    "remote_addr": "127.0.0.1",
    "user_agent": "claude-cli/2.1.319 (external, cli)",
    "headers": { "anthropic-beta": ["claude-code-20250219,oauth-2025-04-20"], "x-app": ["cli"] },
    "caller_key_sha256": "9f86d081…"
  },
  "session": {
    "session_id": "claude:3f2e…",
    "parent_session_id": "",
    "agent_id": "",
    "client_type": "claude",
    "native": true
  },
  "route": {
    "provider": "claude",
    "requested_model": "claude-opus-5-5",
    "upstream_model": "claude-opus-5-5",
    "account_id": "claude-1c859c2c-user@example.com.json",
    "auth_type": "oauth",
    "base_url": "https://api.anthropic.com"
  },
  "outcome": {
    "status": 200,
    "upstream_status": 200,
    "failed": false,
    "error_code": "",
    "error_message": "",
    "stop_reason": "end_turn",
    "latency_ms": 1878,
    "ttft_ms": 412
  },
  "usage": {
    "input_tokens": 1203,
    "output_tokens": 88,
    "reasoning_tokens": 0,
    "cache_read_tokens": 48211,
    "cache_creation_tokens": 1190,
    "cache_creation_5m_tokens": 0,
    "cache_creation_1h_tokens": 1190
  },
  "request": {
    "format": "claude",
    "content_type": "application/json",
    "bytes": 203114,
    "sha256": "…",
    "omitted": false,
    "omitted_reason": ""
  },
  "response": {
    "kind": "sse",
    "content_type": "text/event-stream",
    "bytes": 6120,
    "sha256": "…",
    "omitted": false,
    "omitted_reason": ""
  }
}
```

Field notes:

- `request_id` is the proxy's per-request id (a UUID; the gin request id, `TraceID` in usage records). `attempt` is the 1-based upstream attempt, numbered by the writer in the order the proxy's usage records for that request arrive; a request that fails over writes one record per attempt, all with the same `request_id`. `(request_id, attempt)` is unique. A request that fails before any upstream attempt (for example a 400 from request validation) writes a single record with `attempt` 1 and all-zero `usage`.
- `ts` is when the proxy received the request and is the same on every attempt of that request; `completed_at` is when this attempt's response to the client finished, or when the attempt failed. An attempt's own start time is `completed_at - latency_ms`.
- `client.headers` holds every inbound header except credentials, as lists of values with lower-case names; it is an object, never `null`, and empty when there are none. Credentials are `authorization`, `x-api-key`, `cookie`, `proxy-authorization` and the proxy access-key header; they are never stored. `caller_key_sha256` is the hex SHA-256 of the access key the client presented, or empty.
- `session.*` are the identities the proxy already derives for affinity routing; `native` is whether the request was confirmed as a native Claude Code request and passed through without cloaking.
- `route.account_id` is the proxy's auth id (the credential file name) of the account that served the attempt; empty when no account was selected.
- `outcome.status` is the HTTP status delivered to the client for this attempt (`0` when the attempt produced no client response, as for a failed attempt that was followed by another). `outcome.upstream_status` is the HTTP status the upstream returned for this attempt (`0` when there was no upstream response, for example a transport error), so a 429-then-200 failover reads as `(0, 429)` then `(200, 200)`. `failed`, `error_code` and `error_message` come from the proxy's classification of the attempt. `stop_reason` is copied from the response when the format exposes one.
- `usage.*` come from the proxy's usage record for the attempt; all `0` when none was produced.
- `request.format` is the inbound API format as the proxy classified it: `claude`, `openai-chat`, `openai-responses`, `gemini`, `interactions`, or `unknown`. The archiver's splitter trusts this field and does not guess.
- `response.kind` is `sse`, `json`, `bytes` or `none`.
- `omitted` is `true` when the writer did not store the body (§7.2); `bytes` and `sha256` still describe the original.

### 7.2 Size cap

A record whose bodies together exceed `archive.max-record-bytes` (default 64 MiB) is still written, with the larger body first and then both bodies replaced by empty sections until the record fits, setting `omitted: true` and `omitted_reason: "oversize"` on each omitted body. The turn's existence and metadata are never lost to size.

## 8. Configuration

Both programs read the proxy's `config.yaml`. The archive settings are a top-level `archive` section (not nested under `observability`), with these keys and nothing else the archiver depends on:

```yaml
archive:
  enabled: true
  spool-dir: "/Users/me/.local/share/cliproxyapi/archive-spool"   # absolute path
  segment-max-bytes: 67108864        # integer bytes; default 64 MiB
  segment-max-age: "15m"             # <integer><s|m|h>; default 15m
  ring-segments: 16                  # integer; default 16
  max-record-bytes: 67108864         # integer bytes; default 64 MiB
  r2:
    endpoint: "https://<account-id>.r2.cloudflarestorage.com"
    bucket: "agent-archive"
    prefix: "v1"                     # optional, no leading or trailing slash
    access-key-id: "…"
    secret-access-key: "…"
    sse-c-key: "…"                   # base64 of 32 random bytes; optional, SSE-C is off when absent
```

Rules: keys are exactly as written (no alternative spellings); `segment-max-age` is an integer followed by `s`, `m` or `h`; byte sizes are plain integers; the proxy reads everything except `r2`, which it validates for shape only; the archiver reads `spool-dir` and `r2` and ignores the rest. The archiver also accepts `--spool-dir` and the `r2` values as flags or environment variables for use without a proxy config, with the same names in upper snake case (`ARCHIVE_R2_BUCKET`).

## 9. Versioning

- A change to the header layout beyond offset 12, to the record sections, or to the meaning of an existing meta field bumps `version`. Readers skip frames with a version they do not know, counting them.
- Adding a meta key does not bump the version; readers ignore unknown keys and treat missing keys as their zero value.
- The fixtures directory carries one segment per supported version plus the damage cases of §6. A reader passes when it reproduces `manifest.json` exactly.

## 10. Golden fixtures

`internal/archive/testdata/wal/` (vendored unchanged into the archiver as `fixtures/wal/`):

| File | Contents |
|---|---|
| `seg-000000000001.wal` | Three complete frames: a native Claude Code turn with an SSE response, an OpenAI chat turn with a JSON response, a Gemini turn. |
| `seg-000000000002.wal` | A 429 attempt followed by the successful attempt 2 of the same request, a Responses API turn, then a frame cut mid-payload (truncated tail). |
| `seg-000000000003.wal` | A valid frame, a frame with version 99, a frame with a wrong CRC, a valid frame. Readers yield two records and count one of each skip. |
| `seg-000000000004.wal.open` | An in-progress segment; readers ignore it. |
| `seg-000000000005.wal` | A valid frame, a frame whose `record_len` disagrees with its payload (bad record), a valid frame, then a header with `payload_len` over the limit followed by a valid frame. Readers yield two records, count one bad record, and stop as corrupt without reading the last frame. |
| `seg-000000000006.wal` | Bytes that do not start with the magic. Readers yield nothing and report corrupt. |
| `manifest.json` | For each `.wal`: the expected counts and, per decoded record, `request_id`, `attempt`, `request.format`, and SHA-256 of the meta, request and response sections. |

All bodies are synthetic. The fixtures are regenerated only by the Go test suite (`UPDATE_ARCHIVE_FIXTURES=1 go test ./internal/archive`), and a regeneration is a format change that must be mirrored into the archiver repository in the same change.
