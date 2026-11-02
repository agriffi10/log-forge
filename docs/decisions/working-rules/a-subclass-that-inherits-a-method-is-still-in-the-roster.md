---
type: Decision
title: "A subclass that inherits a method is still in the roster"
description: "scope a roster on defines-or-inherits: keying on *defining* a method made membership a function of where code sits, and dropped five classes out of two lints in one commit with the suite green. (SPEC-038)"
---

**A subclass that inherits a method is still in the roster** — scope keyed on *defining* one makes membership a function of where code happens to sit, and moving five `emit`s into a base dropped those classes out of two lints in a single commit, 34 → 29, with the suite green. Only the sibling roster noticed, and only because it had a floor guard. Both rosters now scope on defines-or-inherits, both carry floors, a class overriding neither `emit` nor `close` may answer from the ancestor whose code it runs, and **a test asserts the two cover the same classes** — they had already drifted twice on trigger and on base spelling. (SPEC-038 FR-001 AC-1a/AC-1b)

## History

### 2026-08-10

- **Settled.**
