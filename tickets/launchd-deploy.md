---
id: launchd-deploy
title: launchd service, logging and health for the archiver
status: todo
priority: p2
dependencies: [vortex-writer-upload]
related: []
scopes: [deploy]
shared_scopes: [project/tickets]
paths: []
tags: [archive, ops]
---
## Goal

The archiver runs unattended on the owner's Mac under launchd next to the proxy, drains the spool as segments complete, and reports its health through a `status` subcommand.

## Work

- `cliproxyapi-archiver run --config <proxy config>` daemon mode: poll the spool every minute and process every completed segment found (segments roll every 15 minutes or 64 MiB, so that is the drain cadence); exponential backoff on R2 errors; graceful shutdown finishes the current segment.
- launchd plist under `~/Library/LaunchAgents/com.tsanterre.cliproxyapi-archiver.plist`, logs to `~/.local/share/cliproxyapi-archiver/logs/` with rotation, install/uninstall script.
- A `status` subcommand printing last segment processed, read stats, last upload time and last error, from a small state file the archiver keeps in the spool directory under an `archiver-` name (allowed by spec section 1; its contents are the archiver's own, not part of the WAL contract).
- Build/deploy script mirroring the proxy's: versioned binaries, symlink swap, rollback.

## Acceptance

Service survives reboot, drains a seeded spool within one poll, backs off and recovers when R2 credentials are temporarily invalid, and `status` shows the last upload within a minute of it happening.
