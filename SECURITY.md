# Security Policy

## Supported versions

`log-foundry` follows semantic versioning, and security fixes land on the **latest released
minor**. There are no long-term support branches: if you are on an older release, the upgrade
path is forward.

| Version | Supported |
|---------|-----------|
| Latest `0.x` release | ✅ |
| Anything older | ❌ — upgrade to the latest release |

Development pre-releases (`X.Y.Z.devN`) are published to PyPI on every merge to `main` so the
publish path stays exercised. They are not intended for production and are not supported;
`pip install log-foundry` does not select them.

## Reporting a vulnerability

**Please do not open a public issue for a security report.**

Use GitHub's private vulnerability reporting on this repository:
[**Report a vulnerability**](https://github.com/agriffi10/log-forge/security/advisories/new).
It creates a private advisory visible only to the maintainers, and it lets us collaborate on a
fix and request a CVE without disclosing the issue first.

If that form is unavailable to you, open a regular issue containing **no detail** — just a
request for a private channel — and we will follow up.

### What to include

- The version of `log-foundry` and the Python version you are running.
- Which sink or feature is involved, if the issue is specific to one.
- The smallest reproduction you can manage, and the impact you believe it has.

### What to expect

- An acknowledgement within **7 days**.
- An assessment of severity and a plan, or an explanation of why we do not consider it a
  vulnerability, within **30 days**.
- Credit in the advisory when a fix ships, unless you prefer otherwise.

We do not operate a bug bounty, and we do not commit to a fixed remediation deadline — the
timeline depends on severity and complexity. We will keep you informed either way.

## Scope

This project is a logging library, so the security properties that matter most are about what it
*writes* and where it writes it. Reports in these areas are especially welcome:

- **Data exposure in log output.** The library deliberately does not capture function arguments
  or return values, and its fallback for an unserializable value is a type-name placeholder
  rather than `repr()`, specifically so that logging cannot leak secrets or PII. A path that
  defeats this is a vulnerability.
- **Credential handling in a sink.** Any sink that logs, echoes, or otherwise exposes the
  credentials or connection strings it was configured with.
- **Injection through log content.** A field value that can alter the structure of what a
  downstream consumer parses.
- **The supply chain.** The published distribution, the release workflow, or the packaging
  metadata.

### Out of scope

- Vulnerabilities in an optional extra's third-party dependency (`boto3`, `psycopg`,
  `confluent-kafka`, and so on). Report those upstream; we will bump the constraint once a fix
  is released.
- Configurations that deliberately log sensitive data — placing a secret into a `fields` value
  puts it in the log, by design.
- The security of the destination you ship logs to. Access control on your SQS queue,
  Elasticsearch cluster, or database is yours to configure.

## Reporting security issues in dependencies

This library has **zero required runtime dependencies**. Everything else is behind an optional
extra and is only installed if you ask for it. If you believe a dependency is compromised rather
than merely vulnerable, please use the private reporting channel above rather than an issue.
