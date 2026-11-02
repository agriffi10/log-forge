---
type: Decision
title: "An open item is closed by being fixed, settled, or recorded as a constraint — never deleted"
description: "a note merely removed takes its reasoning with it, and a reader cannot tell a live defect from a decision that reads like one. Supersede in place, struck through and marked with the spec that closed it. (SPEC-021)"
---

**An open item is closed by being fixed, settled, or recorded as a constraint — never deleted** — a note that is merely removed takes its reasoning with it, and a reader cannot tell a live defect from a decision that reads like one. Superseded notes are struck through in place and marked with the spec that closed them. ~~`architecture.md` §12 carries no open items and §13 states the constraints.~~ — **superseded 2026-09-01.** The rule is one-way by construction: §13 grew from 68 lines to 640 over seventeen specs with nothing pruning it, and unfinished work accreted there among the constraints while §12 read "None". **§12 is what is unfinished** — a defect nobody scheduled or a question left open, each entry naming what would close it — and **§13 is what the design will not do**, a limit it accepts and keeps accepting. A closed item is struck in place with the spec that closed it and its reasoning is *not* repeated in §13: that lives here in Key Decisions and in the delivery doc, and a third copy is a fork with no merge. (SPEC-021, revised 2026-09-01)

## History

### 2026-09-01

- **Reversed in part:** §12 carries live open items after all, and §13 only what the design will not do.

### 2026-07-30

- **Settled** (SPEC-021).
