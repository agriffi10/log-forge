# Best Practices — Index

Domain coding rulebooks for agents. **Read this index first**, then open only the one doc — and only
the section(s) — your task needs. Each doc is a token-efficient, self-contained agent reference with
its **own internal index** — open the doc and read that, rather than trusting a section range
repeated here, which rots silently. This index is a **router, not content** — keep it to one screen.

| Domain | Doc | Load when you are… |
|---|---|---|
| Python (3.12+; PEP 8 + PEP 257/484/526 + Google Python Style Guide) | `python/python.md` | writing or refactoring any Python — style, naming, types, exceptions, docstrings, threading/`contextvars` |

## How to use

1. Match your task to a **Domain** row above.
2. Open that doc and use its **own index** to pick the section IDs you need — read only those.
3. If a rule conflicts with existing code or this repo's config (`ruff` line-length 100, `mypy --strict`),
   follow the repo's config and **flag the conflict** (unless the user says otherwise).

## Adding a new domain

- One file per domain at `best-practices/<domain>/<domain>.md`, written as a token-efficient agent
  reference: a short "how to use" + an internal index + imperative ✅/🔴 rules (match `python/python.md`).
- Add **one row** to the table above and nothing more here — the detail lives in the doc, not the index.
