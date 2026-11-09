---
id: structural-splitter
title: "Byte-exact structural splitter: slice system, tools and messages; hash; reassemble"
status: todo
priority: p1
dependencies: [vortex-writer-upload]
related: []
scopes: [split]
shared_scopes: [project/tickets]
paths: []
tags: [archive, dedupe]
---
## Goal

Given a request body, produce a skeleton plus an ordered list of byte slices for the parts that repeat across turns, such that concatenating skeleton and slices reproduces the original bytes exactly, and each slice has a blake3 hash. Any body the splitter cannot handle is returned whole.

## Work

- A JSON tokenizer that locates top-level keys and array element boundaries without re-encoding (byte offsets only). No `serde_json::Value` round trip on the content.
- Claude Messages first (it is nearly all of the owner's traffic): slice `system` (string or each block), `tools[i]`, `messages[i]`. Every other `request.format` is stored whole in `request_raw` for now; OpenAI chat (`messages[i]`, `tools[i]`), Responses (`instructions`, `input[i]`, `tools[i]`) and Gemini (`systemInstruction`, `tools[i]`, `contents[i]`) follow the same pattern in later tickets. Format comes from the frame's `request.format` field, not guessed.
- Writing the split form means `request_skeleton` + `request_parts` populated and `request_raw` null for that row; rows written before the splitter existed keep `request_raw`. Both shapes are valid forever; `reassemble` handles either.
- Skeleton encoding: the original bytes with each slice replaced by a placeholder token that cannot occur in JSON text; `reassemble(skeleton, slices)` is the inverse.
- Each slice: `kind` (system, tool, message, instructions, content), `role` read from the slice's first key when present, blake3 hash.
- Decline (return raw) on: unknown format, invalid JSON, nesting the tokenizer does not understand, bodies over a size limit.
- Property test: for every fixture body, `reassemble(split(body)) == body` byte-for-byte; fuzz the tokenizer with arbitrary bytes.

## Acceptance

Round-trip property holds on all fixtures and 10k fuzz cases; the splitter is a pure function with no I/O; a benchmark shows under 1 ms per 500 KB body.
