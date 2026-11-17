#!/bin/bash
# Run the Papra webhook probe with a clean receiver log, from the worktree root.
cd "$(dirname "$0")/../../../.." || exit 1
rm -f docs/superpowers/probes/2026-11-17-kreuzakt-contracts/listener.jsonl
.venv/bin/python docs/superpowers/probes/2026-11-17-kreuzakt-contracts/probe_webhooks.py
