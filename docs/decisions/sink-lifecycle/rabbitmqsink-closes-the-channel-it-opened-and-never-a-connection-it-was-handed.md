---
type: Decision
title: "`RabbitMQSink` closes the channel it opened, and never a connection it was handed"
description: "`RabbitMQSink`'s release closes and drops the channel in both modes, and closes and drops the connection only when the sink opened it; a connection passed as `connection=` is left open and its reference kept, so the sink never opens a connection of its own in its place. The ownership test is written once, in `_reset`, which `_release` calls. This speaks for `RabbitMQSink` alone. (SPEC-107)"
---

**Andrew's choice, 2026-11-27: "Fix the behaviour".** The README's *Ownership* rule says an
injected resource is left open for the caller to manage, and `RabbitMQSink.close`'s docstring said
it closes "the channel and, when owned, the connection". The code did otherwise: `_release` closed
the connection with no ownership test, so an application that passed the sink a `pika` connection
it had built found that connection closed at `shutdown()`. Shown the contradiction, he chose the
documentation: the code was the defect. The other way to end it, rewriting the documentation to
say this sink closes what it is handed, was not taken.

**The rule.** The channel is the sink's own in both modes: `_active_channel` opens it on
whichever connection the sink holds, and the sink never borrows one
(*`RabbitMQSink` asks the broker to confirm only when told to, and a lost confirm is unknown*,
`docs/decisions/sink-delivery/`). So every release closes the channel and drops its reference. The
connection is closed, and its reference dropped, only when `_owns_connection` is true. A
connection passed as `connection=` is never closed by the sink: not at `close()`, on any path that
reaches it (`shutdown()`, the exit drain, a swap, a wrapper's forwarded release, the caller's own
call), and not after a failed publish or a failed probe. `confirm` does not enter into it. A
connection the sink opened from `url=` is closed with the sink, as before.

**A borrowed reference is kept, and that is what rules out a leak.** `_active_channel` connects
whenever it finds `self._connection` is `None`. A borrowed sink has no URL, because `url=` is
refused beside `connection=`, so `_connect()` would build `pika`'s default parameters, a broker on
localhost, and nothing would ever close that connection: the sink does not own it and the guard's
one release has already run. With no release storing `None` for a borrowed sink, the reconnect is
reached only by a sink that opened its own connection. The siblings are the precedent: none of
them drops its reference in its release, borrowed or owned.

**The ownership test is written once.** `_reset` states the rule and `_release` calls it, where
`_release` used to restate the release without the test; two copies are how the two sites came to
disagree. `connection_release_sites` in `tests/test_sinks_rabbitmq.py` holds the module to that:
it collects every statement that passes `self._connection` to `_safe_close`, calls
`self._connection.close()` or assigns `None` to `self._connection`, and allows one only inside the
`body`, never the `orelse`, of an `if` whose test is `self._owns_connection` or an `and` with that
as a direct operand. Its bound: it reads those three shapes and no other. It does not follow an
alias of the connection, a keyword argument to `_safe_close`, a tuple assignment such as
`self._channel, self._connection = None, None`, `del self._connection`, a bound
`self._connection.close` handed to another call, or a `with` statement that closes the
connection, and it reports a correct release that follows an early `return` on
`not self._owns_connection`. It is not the proof that a release is right, which is the
behavioural tests beside it. It catches a release site written without the ownership test.

**The refusal after close is unchanged, and it now carries more.** The closed guard refuses a
round from the moment a close begins, keyed on the sink being released and never on who owns the
client (*A sink that released its transport refuses; one that released nothing keeps accepting*,
`docs/decisions/sink-delivery/`), so a closed borrowed sink refuses exactly as a closed owned one
does. With the reference kept, a missing refusal would no longer open a connection nothing reaps:
it would publish on the caller's connection after the close. The `rabbitmq` case of
`test_a_closed_sink_refuses_a_batch_instead_of_reaching_its_driver` is the guard, and it fails
with `DID NOT RAISE SinkDeliveryError` when the refusal is removed from `_round` (run at
`a00162ea` against a scratch copy of `src/`).

**One trade is accepted.** A call that goes around the guard, such as a subclass calling
`_active_channel()` on a closed sink, opens a new channel on the caller's connection, where it
used to open a connection of the sink's own. The connection is the caller's and is still open,
and a channel on it ends when the caller closes it. The alternative was a connection nothing
closes.

**What this does not settle.** It speaks for `RabbitMQSink` alone and rules on no other sink.
`AzureEventHubsSink` and `NATSSink` each close an injected client at close today, against the same
README sentence, and whether those sinks or that sentence's universal wording move has not been
decided (`docs/architecture.md` §12, "`AzureEventHubsSink` and `NATSSink` each close a client the
caller passed in…"). Nor does it
touch a forked child: `RabbitMQSink` compares no pid, so a caller's own `close()` in a child runs
the same release as in the parent (§12, "`RabbitMQSink.close()` in a forked child closes the
channel and the owned connection it inherited").

**Rejected:**

- **Dropping the borrowed reference at close**, as the old `_release` did. It is the leak above.
- **A second ownership test in `_release`.** Two statements of one rule drifted apart once; one
  statement and a guard over the module cannot.
- **An argument that restores the old close.** A caller who wants the connection closed closes it.
- **A roster over every sink that takes an injected client.** None exists, and building one would
  rule on sinks this decision does not change.

**Do NOT** close a borrowed connection in any release; do NOT store `None` over a borrowed
connection's reference; do NOT write the ownership test a second time; do NOT read this as the
rule for another sink.

## History

### 2026-11-27

- **Settled** by SPEC-107, on Andrew's answer of the same day. It supersedes the clause of
  SPEC-010 FR-006 that read "`close()` closes the channel and connection", which carried no
  ownership clause. `tests/test_sinks_rabbitmq.py` and `tests/test_rabbitmq_confirm.py` hold it,
  and `tests/integration/test_rabbitmq.py` against a real broker over a borrowed
  `pika.BlockingConnection`, for both values of `confirm`.
