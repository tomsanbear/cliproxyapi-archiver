---
id: end-to-end-verification
title: "Prove the archive: round-trip, counts and cache hit rate against the live proxy"
status: todo
priority: p1
dependencies: [query-cli, launchd-deploy]
related: []
scopes: [docs]
shared_scopes: [query, project/tickets]
paths: []
tags: [archive, verification]
---
## Goal

Evidence that the archive is complete and exact after a day of real traffic.

## Work

- Run proxy capture and archiver for 24 hours.
- Row count of `turns` equals the proxy's request count for the day (from its logs) minus counted drops.
- For a random sample of 50 request ids, `reassemble` (or `request_raw`) equals the body the client sent, and `response_raw` equals the bytes the client received, both taken from a client-side capture (a logging shim between Claude Code and the proxy). The proxy's own request-log is not a valid oracle because it drops stream chunks under load.
- Cache hit rate per account from the archive matches the proxy's `measure-cache-hit-rate` ticket output.
- Document the results in `docs/verification-<date>.md`.

## Acceptance

All four checks pass and are recorded; any discrepancy becomes a ticket.
