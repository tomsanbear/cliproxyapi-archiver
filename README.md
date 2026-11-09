# cliproxyapi-archiver

Archives every conversation turn that passes through [CLIProxyAPI](https://github.com/tomsanbear/CLIProxyAPI). The proxy appends raw turn records to a local ring-buffered write-ahead log; this daemon drains that log, deduplicates the resent message history byte-exactly, writes [Vortex](https://github.com/vortex-data/vortex) files to a Cloudflare R2 bucket, and queries them with [DataFusion](https://github.com/apache/datafusion).

## How it fits together

```
agents → CLIProxyAPI (Go) → spool/seg-*.zst → cliproxyapi-archiver (Rust) → R2 (turns/, blobs/ as .vortex)
                                                        └── query: DataFusion + vortex-datafusion
```

- The proxy never talks to R2. Its only archive work is a local file append, bounded by a 1 GB ring of 64 MB segments; when the ring is full the oldest segment is dropped and counted.
- The archiver drains oldest-first, newest-first above a high-water mark, and deletes a segment only after its rows are confirmed in R2.
- Request bodies are sliced at structural boundaries (`system`, `tools`, each message) without re-serializing. Each unique slice is stored once in `blobs`; a turn stores a skeleton plus the ordered hashes, so the exact original bytes reassemble by concatenation. Bodies the splitter cannot handle are stored whole.
- Objects are encrypted with a customer-provided key (R2 SSE-C) until Vortex ships segment encryption ([vortex-data/vortex#10968](https://github.com/vortex-data/vortex/pull/10968)).

The WAL format is specified in `docs/archive-wal-format.md`, shared verbatim with the proxy fork, with golden fixtures under `fixtures/wal/`.

## Status

Scaffolding. Work is tracked as [ticketsplease](https://github.com/moderately-ai/ticketsplease) tickets in `tickets/`; run `tkt rollup --tag archive`.

## License

MIT.
