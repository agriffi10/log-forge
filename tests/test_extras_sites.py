"""SPEC-071 FR-002 — every lazy import of an optional extra, with that extra made absent.

The table is the seventeen sites an AST scan of `src/log_foundry/**/*.py` finds on `2368ade3`, the
commit SPEC-071 was built from: every import of a module outside the standard library and the
package, outside `if TYPE_CHECKING:`. Each row reaches its site through the call a caller makes and
asserts what holds there with the module absent.

**Recorded on the unmodified tree first** (Phase 1), asserting only what was already true: a bare
site raises an `ImportError` whose `.name` is the module or one of its dotted parents, and a soft
site falls back. The parent clause is not slack: which component an import finds missing depends
on what is installed. `azure.eventhub` is missing as `azure` where nothing provides that namespace
and as `azure.eventhub` where a left-over `azure/` directory does, and `from google.cloud import
pubsub_v1` reports `google.cloud` both where `google.cloud` is absent and where the submodule is.

**Absence is made by a finder, not by `sys.modules[name] = None`,** whose message is `import of
boto3 halted; None in sys.modules` rather than the interpreter's `No module named 'boto3'`. The
finder sits first on `sys.meta_path` and raises for the module and its submodules, which are
removed from `sys.modules`, and the leaf attribute is removed from a parent package still
imported. That last step matters where the extras ARE installed (`integration.yml` runs this suite
with every extra present): `from google.cloud import pubsub_v1` reads an already-imported
submodule off its parent and never imports, so a row that only cleared `sys.modules` would build a
real client.

Serves invariant 13: a missing extra is refused at the constructor that needs it. The soft rows
serve invariant 2's neighbour rule that a missing optional speed-up costs nothing.
"""

from __future__ import annotations

import sqlite3
import sys
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pytest

from log_foundry.sinks import eventhubs, nats, rabbitmq, sentry
from log_foundry.sinks._extras import MissingExtraError
from log_foundry.sinks.clickhouse import ClickHouseSink
from log_foundry.sinks.eventhubs import AzureEventHubsSink
from log_foundry.sinks.firehose import FirehoseSink
from log_foundry.sinks.kafka import KafkaSink
from log_foundry.sinks.kinesis import KinesisSink
from log_foundry.sinks.mongodb import MongoDBSink
from log_foundry.sinks.nats import NATSSink
from log_foundry.sinks.postgres import PostgresSink
from log_foundry.sinks.pubsub import GooglePubSubSink
from log_foundry.sinks.rabbitmq import RabbitMQSink
from log_foundry.sinks.redis import RedisListSink, RedisStreamsSink
from log_foundry.sinks.sentry import SentrySink
from log_foundry.sinks.sns import SNSSink
from log_foundry.sinks.sqlite import SQLiteSink
from log_foundry.sinks.sqs import SQSSink
from test_sinks_nats import FakeNATS

if TYPE_CHECKING:
    from collections.abc import Callable
    from importlib.machinery import ModuleSpec


class AbsentFinder:
    """A meta-path finder that raises for one module and its submodules.

    Attributes:
      name: The module made absent.
      raised: Every exception this finder raised, in order, so a test can compare identity.
    """

    def __init__(self, name: str, make: Callable[[str], BaseException] | None = None) -> None:
        """Arms the finder for ``name``.

        Args:
          name: The module made absent, with its submodules.
          make: Builds the exception for a requested name; the interpreter's own
            ``ModuleNotFoundError`` shape when omitted.

        Returns:
          None.

        Raises:
          None.
        """
        self.name = name
        self.raised: list[BaseException] = []
        self._make = make or (
            lambda full: ModuleNotFoundError(f"No module named {full!r}", name=full)
        )

    def find_spec(
        self, fullname: str, path: object = None, target: object = None
    ) -> ModuleSpec | None:
        """Raises for the absent module or one of its submodules, and declines anything else.

        Args:
          fullname: The module being imported.
          path: Unused.
          target: Unused.

        Returns:
          ``None`` for a module this finder does not cover.

        Raises:
          BaseException: The configured exception, for a covered module.
        """
        if fullname == self.name or fullname.startswith(self.name + "."):
            error = self._make(fullname)
            self.raised.append(error)
            raise error
        return None


def make_absent(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    make: Callable[[str], BaseException] | None = None,
) -> AbsentFinder:
    """Makes ``name`` and its submodules unimportable for the rest of the test.

    Args:
      monkeypatch: Undoes every change at teardown.
      name: The module to make absent.
      make: The finder's exception factory, if not the interpreter's own shape.

    Returns:
      The installed finder.

    Raises:
      None.
    """
    for key in [k for k in sys.modules if k == name or k.startswith(name + ".")]:
        parent, _, leaf = key.rpartition(".")
        if parent and parent in sys.modules:
            monkeypatch.delattr(sys.modules[parent], leaf, raising=False)
        monkeypatch.delitem(sys.modules, key, raising=False)
    finder = AbsentFinder(name, make)
    monkeypatch.setattr(sys, "meta_path", [finder, *sys.meta_path])
    return finder


def self_and_parents(module: str) -> set[str]:
    """Returns ``module`` and each of its dotted parents.

    Args:
      module: A dotted module name.

    Returns:
      ``{"a", "a.b", "a.b.c"}`` for ``"a.b.c"``.

    Raises:
      None.
    """
    parts = module.split(".")
    return {".".join(parts[: i + 1]) for i in range(len(parts))}


class _Connection:
    """A connection stand-in that records nothing and is never used."""

    def close(self) -> None:
        """Closes nothing.

        Args:
          None.

        Returns:
          None.

        Raises:
          None.
        """


@dataclass(frozen=True)
class Site:
    """One lazy import of an extra, and the call that reaches it.

    Attributes:
      where: ``file:qualname`` of the importing function.
      module: The module ``import_extra`` is given.
      extra: The ``[project.optional-dependencies]`` key that installs it.
      reach: Makes the call a caller makes to reach the site.
    """

    where: str
    module: str
    extra: str
    reach: Callable[[], Any]


BARE: tuple[Site, ...] = (
    Site(
        "clickhouse.py:ClickHouseSink.__init__",
        "clickhouse_connect",
        "clickhouse",
        lambda: ClickHouseSink("t", dsn="clickhouse://x"),
    ),
    Site(
        "eventhubs.py:AzureEventHubsSink.__init__",
        "azure.eventhub",
        "azure-eventhubs",
        lambda: AzureEventHubsSink(connection_str="Endpoint=sb://x/", eventhub="h"),
    ),
    Site(
        "eventhubs.py:_event_data_cls",
        "azure.eventhub",
        "azure-eventhubs",
        lambda: eventhubs._event_data_cls(),
    ),
    Site("firehose.py:FirehoseSink.__init__", "boto3", "aws", lambda: FirehoseSink("s")),
    Site(
        "kafka.py:KafkaSink.__init__",
        "confluent_kafka",
        "kafka",
        lambda: KafkaSink("t", bootstrap_servers="b:9092"),
    ),
    Site("kinesis.py:KinesisSink.__init__", "boto3", "aws", lambda: KinesisSink("s")),
    Site(
        "mongodb.py:MongoDBSink.__init__",
        "pymongo",
        "mongo",
        lambda: MongoDBSink(uri="mongodb://x", database="d", collection="c"),
    ),
    Site("nats.py:NATSSink.__init__", "nats", "nats", lambda: NATSSink("s")),
    Site(
        "postgres.py:PostgresSink._connect",
        "psycopg",
        "postgres",
        lambda: PostgresSink("t", dsn="postgresql://x"),
    ),
    Site(
        "pubsub.py:GooglePubSubSink.__init__",
        "google.cloud.pubsub_v1",
        "gcp-pubsub",
        lambda: GooglePubSubSink("t"),
    ),
    Site(
        "rabbitmq.py:RabbitMQSink._connect",
        "pika",
        "amqp",
        lambda: RabbitMQSink(exchange="e", routing_key="r", url="amqp://x"),
    ),
    Site("redis.py:_RedisSink.__init__", "redis", "redis", lambda: RedisListSink("k")),
    Site("sns.py:SNSSink.__init__", "boto3", "aws", lambda: SNSSink("arn")),
    Site("sqs.py:SQSSink.__init__", "boto3", "aws", lambda: SQSSink("https://q")),
)
"""The fourteen sites that raise when their extra is absent."""

SOFT: tuple[tuple[str, str, str], ...] = (
    ("mongodb.py:MongoDBSink._resolve_bson", "bson", "mongo"),
    ("rabbitmq.py:RabbitMQSink._persistent_properties", "pika", "amqp"),
    ("sentry.py:_import_sdk", "sentry_sdk", "sentry"),
)
"""The three sites that fall back instead: ``where``, module, extra."""


def _nats_loop_threads() -> set[threading.Thread]:
    """Returns the live NATS loop threads.

    Args:
      None.

    Returns:
      Every alive thread carrying the NATS loop thread's name.

    Raises:
      None.
    """
    return {t for t in threading.enumerate() if t.name == nats._LOOP_THREAD_NAME and t.is_alive()}


def test_the_table_is_the_seventeen_sites() -> None:
    """The count the AST scan found on ``2368ade3``; the roster test holds it by tuple."""
    wheres = [site.where for site in BARE] + [where for where, _, _ in SOFT]
    assert len(wheres) == 17
    assert len(set(wheres)) == 17


@pytest.mark.parametrize("site", BARE, ids=[site.where for site in BARE])
def test_a_bare_site_raises_an_import_error_naming_the_module(
    monkeypatch: pytest.MonkeyPatch, site: Site
) -> None:
    before = _nats_loop_threads()
    make_absent(monkeypatch, site.module)
    with pytest.raises(ImportError) as info:
        site.reach()
    assert info.value.name in self_and_parents(site.module), info.value
    assert not (_nats_loop_threads() - before), "a refused NATSSink left its loop thread running"


def test_mongodb_without_bson_resolves_no_encoder(monkeypatch: pytest.MonkeyPatch) -> None:
    make_absent(monkeypatch, "bson")
    sink = MongoDBSink(client={"d": {"c": object()}}, database="d", collection="c")
    sink._resolve_bson()
    assert sink._bson_resolved
    assert sink._bson_encode is None


def test_rabbitmq_without_pika_uses_its_own_properties(monkeypatch: pytest.MonkeyPatch) -> None:
    make_absent(monkeypatch, "pika")
    sink = RabbitMQSink(exchange="e", routing_key="r", connection=_Connection())
    assert isinstance(sink._persistent_properties(), rabbitmq._PersistentProperties)


def test_sentry_without_the_sdk_imports_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    make_absent(monkeypatch, "sentry_sdk")
    assert sentry._import_sdk() is None


# --- Phase 2: the sites through `import_extra` ----------------------------------------------------


@pytest.mark.parametrize("site", BARE, ids=[site.where for site in BARE])
def test_a_bare_site_names_its_extra_and_the_command(
    monkeypatch: pytest.MonkeyPatch, site: Site
) -> None:
    make_absent(monkeypatch, site.module)
    with pytest.raises(MissingExtraError) as info:
        site.reach()
    message = str(info.value)
    assert f'The "{site.extra}" extra is not installed (' in message
    assert f'pip install "log-foundry[{site.extra}]"' in message
    assert "No module named" in message
    assert info.value.name in self_and_parents(site.module)


def test_an_injected_eventhubs_producer_refuses_a_missing_extra_at_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-002: it used to construct and then raise ``ModuleNotFoundError`` from every emit."""
    make_absent(monkeypatch, "azure.eventhub")
    with pytest.raises(MissingExtraError) as info:
        AzureEventHubsSink(producer=object())
    assert 'pip install "log-foundry[azure-eventhubs]"' in str(info.value)
    monkeypatch.setattr(eventhubs, "_event_data_cls", lambda: object)
    assert AzureEventHubsSink(producer=object()).producer is not None


def test_eventhubs_with_neither_producer_nor_connection_string_keeps_its_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An argument error is reported ahead of an environment one."""
    make_absent(monkeypatch, "azure.eventhub")
    with pytest.raises(ValueError, match="requires connection_str when no producer is injected"):
        AzureEventHubsSink()


_EXTRA_MODULES = sorted({site.module for site in BARE} | {module for _, module, _ in SOFT})
"""Every module an import site names, derived from the two tables."""


def test_an_injected_client_imports_no_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """FR-002: with every extra absent, each injected-client sink still constructs.

    The finder raises for any import of an extra, so a sink that reached for one would fail here;
    the one exception is ``AzureEventHubsSink``'s ``EventData``, which is patched.
    """
    for module in _EXTRA_MODULES:
        make_absent(monkeypatch, module)
    monkeypatch.setattr(eventhubs, "_event_data_cls", lambda: object)
    connection = sqlite3.connect(":memory:")
    built = [
        ClickHouseSink("t", client=object()),
        AzureEventHubsSink(producer=object()),
        FirehoseSink("s", client=object()),
        KafkaSink("t", producer=object()),
        KinesisSink("s", client=object()),
        MongoDBSink(client={"d": {"c": object()}}, database="d", collection="c"),
        PostgresSink("t", connection=object()),
        GooglePubSubSink("t", client=object()),
        RabbitMQSink(exchange="e", routing_key="r", connection=_Connection()),
        RedisStreamsSink("s", client=object()),
        RedisListSink("k", client=object()),
        SentrySink(client=object()),
        SNSSink("arn", client=object()),
        SQLiteSink(":memory:", connection=connection),
        SQSSink("https://q", client=object()),
    ]
    nats_sink = NATSSink("s", client=FakeNATS())
    try:
        assert len(built) + 1 == 16, "the sixteen injected-client classes"
    finally:
        nats_sink.close()
        connection.close()
