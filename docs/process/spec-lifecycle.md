# The spec lifecycle

Specs move **Draft → In Progress → Completed** (the status in each spec's header is authoritative; the
`INDEX.md` row mirrors it).

- **Draft** — written, **reviewed by a fresh-context reviewer (`reviewer-contract.md`)** and
  refined, but **do not build until told.** Specs are often authored well ahead of implementation. A
  Draft spec sitting in the repo is not a signal to start it. A spec is not Draft-ready while it still
  has unresolved questions (`authoring-a-spec.md`), and a spec that has not been through the gate is not Draft-ready,
  whatever its header says.
- **In Progress** — exactly one spec at a time is in flight. Set when you branch to build it.
- **Completed** — merged on green CI, delivery doc written (`completion-ritual.md`).

**Arcs.** Related specs can be grouped into *arcs* with an explicit **build order** documented in
`INDEX.md`. Grouping is a choice; recording the order of a **size split** (`authoring-a-spec.md`) is not — a spec cut
for size always leaves an arc entry behind. Build in that order; arcs can have non-obvious
dependencies. An arc is also what an over-scoped spec *becomes*: a spec that runs past the size
ceiling in `authoring-a-spec.md` is cured by a second spec beside it and an arc entry holding the order, rather than
by a longer spec.

