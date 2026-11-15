---
type: Decision
title: "Sessions keep Docker images cached"
description: "an agent session removes the containers and networks it started but never removes a Docker image, so the next session does not pull it again; `.claude/settings.json` denies `docker rmi`, `docker image rm`, every prune and `docker compose … --rmi`. Do not add image removal to a cleanup; reopening this is Andrew's call. (Andrew, 2026-11-15)"
---

Several agent sessions share this machine's Docker daemon, and the integration suite and the
audit's Linux probes pull the same pinned images. On 2026-11-15, while permission rules were being
drafted so that sessions in auto mode stop prompting for routine Docker commands, Andrew first
asked that sessions also clean up "images/containers when done", then withdrew the image half:
"actually scratch that, because I don't want fresh pulls or every sesseion". So a session removes
what it started and leaves images in the local cache.

**What the decision accepts.** The local image cache grows with each new pinned digest and is never
trimmed by a session; trimming it is a person's job, done outside a session.

**How it is held.** `.claude/settings.json` denies `docker rmi`, `docker image rm`, any `docker …
prune`, and `docker compose … --rmi`, so none of them runs without the person approving it, even
in auto mode. The procedure sessions follow is in `docs/process/several-agents.md`, under the
shared Docker daemon.

**Do NOT build:** image removal in any session cleanup, script or hook. Reopening this is Andrew's
call.

## History

### 2026-11-15

- **Settled** by Andrew, in chat, while the Docker permission rules were drafted.
