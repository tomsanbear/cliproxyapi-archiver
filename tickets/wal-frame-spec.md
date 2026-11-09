---
id: wal-frame-spec
title: Adopt the CLIProxyAPI archive WAL spec and vendor its golden fixtures
status: done
priority: p1
dependencies: []
related: []
scopes: [spec]
shared_scopes: [wal, project/tickets]
paths: []
tags: [archive, spec]
---
## Goal

This repo carries a copy of `docs/archive-wal-format.md` from the proxy fork (ticket `archive-wal-frame-spec` there) and the identical golden fixtures under `fixtures/wal/`, so the Rust reader is tested against exactly what the Go writer is tested against.

## Work

- Copy the spec verbatim with its version; record the proxy commit it came from in `docs/WAL-SPEC-SOURCE`.
- Vendor `internal/archive/testdata/` from the proxy as `fixtures/wal/`; add a script that re-syncs it and fails if the two diverge.
- Public repo: fixtures must contain no real conversation content. Generate synthetic bodies that exercise every format the splitter handles (Claude, OpenAI chat, Responses, Gemini), plus a truncated-tail segment and an unknown-version frame.

## Acceptance

`cargo test` runs the reader over `fixtures/wal/`; the truncated-tail fixture yields N-1 frames; the unknown-version frame is counted and skipped. The sync script reports no divergence from the proxy fork at the recorded commit.
