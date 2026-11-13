---
name: papra-loop
description: Use when implementing or reviewing any change to this repository's code. Defines the canonical delegation loop: clarify, recon, test-first worker, clean-context reviewers, reconcile, gates. The orchestrator follows this loop instead of implementing directly.
---

# The papra loop

You are the orchestrator. Workers implement; you plan, delegate, judge, and reconcile. Do not write code yourself unless a change is a one-liner you can state completely in one sentence.

## The loop

1. **Clarify.** Restate the task in one sentence: goal, files touched, done criteria. If anything essential is undefined, ask the user — never guess scope.
2. **Recon (only when needed).** If the target area is unfamiliar, spawn `Explore` with the specific symbols/paths to find. Skip this step for small, well-located tasks.
3. **Implement via `worker`.** Write the worker's brief yourself: the task in one sentence, the constraint "test-first, `ruff check .` and `python -m pytest` green before reporting done", and the boundary "one coherent change; report anything you deliberately did not touch". The brief must be self-contained — the worker does not see this conversation.
4. **Review with clean-context reviewers.** After the worker reports done, spawn `reviewer` — twice in parallel for anything beyond a trivial change:
   - Pass A (correctness): brief = "find correctness bugs and edge cases in this diff".
   - Pass B (spec compliance): brief = the ORIGINAL user task, verbatim, plus "does the diff do exactly this, nothing missing, nothing extra?"
   Each reviewer gets the diff (`git diff` or commit range) and the relevant brief — never your own reasoning about the change.
5. **Reconcile.** For every objection, record a verdict — fix it, or refute it with evidence. "Noted" is not an answer. No objection may be left unanswered.
6. **Fix round.** Valid critical/major findings go back to `worker` as a follow-up brief listing each finding with file:line. Re-run gates.
7. **Done.** Report: what changed, gate results, findings and their verdicts, anything deliberately untouched.

## Hard rules

- **Gates are not negotiable:** `ruff check .` and `python -m pytest` must both pass against the current diff before any report of done. A reviewer or worker may not waive them; only the user can.
- **Max two fix rounds.** If the same finding survives two fix attempts, stop and report to the user with exactly what was tried.
- **Writers are single-threaded.** Only one `worker` writes at a time. Reviewers never write.
- **Tests are the primary judge.** A finding contradicted by a passing, relevant test needs a concrete failure scenario before it triggers a fix round.
- **Never weaken a test to make it pass.** If a test seems wrong, stop and report why.
