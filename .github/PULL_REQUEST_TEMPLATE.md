## Summary
<!-- What does this PR do? One or two sentences. -->

Closes #<!-- issue number -->

## Spec & plan
<!-- Delete this section for non-spec changes (chores, docs, fixes). -->
- Spec: SPEC-XXX
- [ ] Maps to the validated implementation plan for this spec (no scope creep)
- [ ] No new Open Questions introduced — emergent issues were resolved in-session (spec updated if scope changed)

## Changes
<!-- Bullet list of what changed and why. -->
- 

## Review (before this branch was pushed)
- [ ] **Two** fresh-context reviews of this diff, in **different frames**, before the push — every
      diff gets two, including a spec-only or docs-only one (`docs/process.md` §3 names the two
      frames for a diff with no code in it). The single review a spec or plan gets is the earlier
      gate on the artifact, not this one.
- [ ] Every finding **fixed or flagged** out loud; nothing dropped silently
- [ ] Any acceptance criterion that could not settle pre-push is listed under **Owed** below

### Owed (criteria that can only settle on the green run)
<!-- One line each, or "none". These block the merge, not the push. Edit this line —
     "none" is a claim you are making, not a placeholder left unfilled. -->
- none

## Testing
<!-- How was this tested? What should the reviewer verify? -->
- [ ] Tested locally
- [ ] Added / updated tests
- [ ] No new tests needed — explain why:

## Checklist
- [ ] Code follows the project's style and conventions
- [ ] `ruff`, `mypy`, `pytest`, `sh scripts/spec-lint.sh` and `sh scripts/docs-lint.sh` pass
- [ ] `sh scripts/docs-lint-test.sh` passes (only if you changed the linter)
- [ ] Documentation updated if applicable
- [ ] No unrelated changes included

## Landing
- [ ] Watching this PR to green — will merge once CI passes **and** every Owed item above is settled
- [ ] Will confirm `main` is green after merge (fix immediately with a new PR if it isn't)