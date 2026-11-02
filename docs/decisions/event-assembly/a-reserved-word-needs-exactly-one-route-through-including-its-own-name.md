---
type: Decision
title: "A reserved word needs exactly one route through, including its own name"
description: "`fields` is the third reserved word and `fields={\"fields\": …}` must work. The keyword form wins a collision, and the merge **absorbs** a non-mapping rather than raising. (SPEC-025, SPEC-034)"
---

**A reserved word needs exactly one route through, including its own name** — `echo` and `message` were parameters stealing ordinary words from the field namespace, and `fields=` is the escape hatch, so `fields` becomes the third reserved word and `fields={"fields": …}` must work. The keyword form wins a collision (`{**base, **overrides}`), and the merge **absorbs** a non-mapping rather than raising: it runs in the emitter, outside `api._log`'s orphan guard, so an unguarded merge broke SPEC-025's promise on all four paths. (SPEC-034 FR-004)

## History

### 2026-08-10

- **Settled.**
