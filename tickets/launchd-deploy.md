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

The archiver runs unattended on the owner's Mac under launchd next to the proxy, drains the spool on schedule, and reports its health where the proxy's management panel shows it.

## Work

- `cliproxyapi-archiver run --config <proxy config>` daemon mode: drain on the hour boundary or when the spool passes 512 MB, whichever first; exponential backoff on R2 errors; graceful shutdown finishes the current batch.
- launchd plist under `~/Library/LaunchAgents/com.tsanterre.cliproxyapi-archiver.plist`, logs to `~/.local/share/cliproxyapi-archiver/logs/` with rotation, install/uninstall script.
- Status file for the panel (`archiver-status.json`) and a `status` subcommand.
- Build/deploy script mirroring the proxy's: versioned binaries, symlink swap, rollback.

## Acceptance

Service survives reboot, drains a seeded spool within one cycle, backs off and recovers when R2 credentials are temporarily invalid, and the panel shows last upload time within a minute of it happening.
