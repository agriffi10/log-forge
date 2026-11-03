---
type: Decision
title: "A value too large to *render* is replaced, never clipped"
description: "an over-long int becomes `<int: ~N digits>`; a truncated number is silently wrong. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error it prevents. (SPEC-020)"
---

**A value too large to *render* is replaced, never clipped** — `int` is the one type with no natural ceiling, and CPython refuses to render one past `sys.get_int_max_str_digits()`, so an over-long integer becomes `<int: ~N digits>`. Truncating digits would silently change the number, and a wrong number is worse than a visibly elided one. Detection is `bit_length()`, never `len(str(v))` — the obvious check raises the very error being prevented — with the ratio rounded so it errs toward replacing. (SPEC-020)

## History

### 2026-07-30

- **Settled.**
