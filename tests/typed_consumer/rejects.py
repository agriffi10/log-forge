"""The consumer calls SPEC-051 makes into `mypy --strict` errors, one per `# want:` line.

Not run as a test, and not expected to type-check -- `tests/test_typed_consumer.py` asserts that
mypy reports exactly these and nothing else. A corpus of only-passes cannot tell a probe that is
working from one that has stopped resolving the library's types, which passes in silence.

Each marker is matched against mypy's message text rather than its error code, because
`call-arg` covers both "too many positional arguments" and a misspelled keyword -- so a code-only
assertion is satisfied by the wrong defect. The runner reads them with `tokenize`, taking COMMENT
tokens only: a first version matched them by regex and picked up this very paragraph.
"""

from log_foundry import Config, ContinueResult, FlushResult, Health, SinkLosses, configure, trace
from log_foundry.sinks.datadog import DatadogSink
from log_foundry.sinks.elasticsearch import ElasticsearchSink
from log_foundry.sinks.logstash import LogstashSink
from log_foundry.sinks.splunk import SplunkHECSink

# FR-001: field order is not a contract any more, so a positional form is refused.
_a = Health(0, 0, 0)                       # want: Too many positional arguments for "Health"
_b = SinkLosses(0, 0)                      # want: Too many positional arguments for "SinkLosses"
_c = FlushResult(True)                     # want: Too many positional arguments for "FlushResult"
_d = ContinueResult(True)                  # want: Too many positional arguments for "ContinueResult"
_e = Config("svc")                         # want: Too many positional arguments for "Config"

# ...and a misspelled keyword is the OTHER `call-arg`, which is why the codes are not enough.
_f = Health(faled_batches=0)               # want: Unexpected keyword argument "faled_batches" for "Health"

# FR-002: widened to `Mapping[str, object]`, not to anything at all.
configure(defaults=3)                      # want: Argument "defaults" to "configure" has incompatible type "int"
trace(defaults=3)                          # want: No overload variant of "trace" matches argument type "int"

# FR-005: the suppression that used to blind every one of these.
_g = DatadogSink("k", timeout="not-a-float")   # want: Argument "timeout" to "DatadogSink" has incompatible type "str"
_h = ElasticsearchSink("u", index="i", max_batch_bytes="lots")  # want: Argument "max_batch_bytes" to "ElasticsearchSink" has incompatible type "str"
_i = SplunkHECSink("u", "tok", nonsense=1)     # want: Unexpected keyword argument "nonsense" for "SplunkHECSink"

# ...and the two that were already wrong at runtime: a duplicate argument, and a keyword socket
# mode discards in silence.
_j = DatadogSink("k", body_format="ndjson")    # want: Unexpected keyword argument "body_format" for "DatadogSink"
_k = LogstashSink(host="h", port=1, unknown=2) # want: Unexpected keyword argument "unknown" for "LogstashSink"
