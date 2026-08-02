#!/usr/bin/env python3
"""Build a CycloneDX SBOM for a built distribution (SPEC-023 FR-001, FR-002).

Why this is a script and not four lines of workflow YAML: it does three things that each have a
way to be silently wrong, and all three are worth being able to run locally.

1. **It describes the published artifact, not this checkout.** The wheel in ``dist/`` is installed
   into a throwaway virtualenv together with every optional extra, and the SBOM is taken from that
   environment. That closure is what a consumer actually gets from
   ``pip install log-foundry[...]``.

2. **The generating tool runs from a different virtualenv than the one it describes.** Otherwise
   ``cyclonedx-bom`` and its ~30 dependencies appear as components of the library's own SBOM.
   ``cyclonedx-py environment`` takes the path of the environment to describe, which is exactly the
   separation needed. Its version is read from ``poetry.lock`` so the pin stays the one Dependabot
   maintains (FR-007) rather than drifting to whatever PyPI serves today.

3. **It writes ``metadata.component`` itself.** ``cyclonedx-py environment`` emits none, and the
   ``poetry`` subcommand — which does — cannot read this project at all: it takes the root component
   from ``[tool.poetry].name``, which does not exist here because the name lives in ``[project]``
   under PEP 621. See the FR-001 amendment in the spec. The version comes from the sdist filename,
   the same value ``release.yml`` already parses for its tag-agreement check, so the SBOM cannot
   disagree with what was published.

The assertions at the end are the point of the exercise: an SBOM that is empty, that names version
``0.0.0``, or that has leaked the build's own tooling into the component list is worse than no SBOM,
because it looks authoritative. Any of those exits non-zero.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import tomllib
import venv
from pathlib import Path

# Packages that must never appear as components: they build or audit the library, they are not part
# of what a consumer installs. A leak means the wrong virtualenv was described.
_BUILD_ONLY = frozenset({"pytest", "pytest-asyncio", "pytest-cov", "ruff", "mypy", "cyclonedx-bom",
                         "pip-audit", "build", "twine"})

# A floor, not an inventory: enough to prove the extras were resolved rather than skipped. The exact
# component count moves with every transitive release and is not worth asserting on.
_EXPECTED = frozenset({"boto3", "confluent-kafka", "psycopg", "pymongo", "sentry-sdk"})

# Virtualenv scaffolding. These are present because something had to install the wheel, not because
# the library depends on them; listing them as dependencies of `log-foundry` would be a false claim.
_INSTALLER = frozenset({"pip", "setuptools", "wheel"})

_SDIST = re.compile(r"^log_foundry-(?P<version>.+)\.tar\.gz$")


def _die(message: str) -> None:
    print(f"::error::make-sbom: {message}", file=sys.stderr)
    raise SystemExit(1)


def version_from_dist(dist_dir: Path) -> str:
    """Derive the version from the sdist filename — the artifact that was actually built."""
    sdists = sorted(dist_dir.glob("log_foundry-*.tar.gz"))
    if len(sdists) != 1:
        _die(f"expected exactly one sdist in {dist_dir}, found {len(sdists)}")
    match = _SDIST.match(sdists[0].name)
    if match is None:  # pragma: no cover - guarded by the glob above
        _die(f"cannot parse a version out of {sdists[0].name}")
        raise AssertionError  # unreachable; keeps mypy's narrowing honest
    return match.group("version")


def wheel_in(dist_dir: Path) -> Path:
    wheels = sorted(dist_dir.glob("log_foundry-*.whl"))
    if len(wheels) != 1:
        _die(f"expected exactly one wheel in {dist_dir}, found {len(wheels)}")
    return wheels[0]


def extras_from(pyproject: Path) -> list[str]:
    """Every key of ``[project.optional-dependencies]``.

    Read rather than hardcoded: a twelfth extra added later must appear in the SBOM without anyone
    remembering to update this script.
    """
    data = tomllib.loads(pyproject.read_text())
    extras = sorted(data.get("project", {}).get("optional-dependencies", {}))
    if not extras:
        _die(f"no [project.optional-dependencies] found in {pyproject}")
    return extras


def locked_version(lock: Path, package: str) -> str:
    """The version of ``package`` recorded in poetry.lock, so the tool pin is the maintained one."""
    data = tomllib.loads(lock.read_text())
    for entry in data.get("package", []):
        if entry.get("name") == package:
            return str(entry["version"])
    _die(f"{package} is not in {lock}; is the `security` group still present?")
    raise AssertionError  # unreachable


def _pip(env_dir: Path, *args: str) -> None:
    subprocess.run([str(env_dir / "bin" / "pip"), "install", "--quiet", *args], check=True)


_VALIDATE = """
import sys
from cyclonedx.schema import SchemaVersion
from cyclonedx.validation.json import JsonStrictValidator
errors = JsonStrictValidator(SchemaVersion.from_version(sys.argv[2])).validate_str(
    open(sys.argv[1]).read()
)
if errors:
    print(errors, file=sys.stderr)
    raise SystemExit(1)
"""


def _validate(tooling: Path, sbom_file: Path, spec_version: str) -> None:
    """Fail if the edited document is not valid CycloneDX for the version it declares."""
    result = subprocess.run(
        [str(tooling / "bin" / "python"), "-c", _VALIDATE, str(sbom_file), spec_version],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        _die(f"the generated SBOM is not schema-valid: {result.stderr.strip()}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist-dir", type=Path, default=Path("dist"))
    parser.add_argument("--pyproject", type=Path, default=Path("pyproject.toml"))
    parser.add_argument("--lock", type=Path, default=Path("poetry.lock"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    version = version_from_dist(args.dist_dir)
    if version == "0.0.0":
        _die("the built version is the 0.0.0 placeholder; poetry-dynamic-versioning did not run")
    extras = extras_from(args.pyproject)
    wheel = wheel_in(args.dist_dir)

    with tempfile.TemporaryDirectory() as tmp:
        described = Path(tmp) / "described"   # what a consumer installs
        tooling = Path(tmp) / "tooling"       # the SBOM generator, kept out of the above
        venv.create(described, with_pip=True)
        venv.create(tooling, with_pip=True)

        print(f"installing {wheel.name} with extras: {', '.join(extras)}")
        _pip(described, f"{wheel}[{','.join(extras)}]")
        _pip(tooling, f"cyclonedx-bom=={locked_version(args.lock, 'cyclonedx-bom')}")

        raw = Path(tmp) / "raw.cdx.json"
        subprocess.run(
            [str(tooling / "bin" / "cyclonedx-py"), "environment", str(described),
             "--mc-type", "library", "--output-file", str(raw)],
            check=True,
        )
        sbom = json.loads(raw.read_text())

        # `log-foundry` is the subject of this document, not one of its dependencies. It appears in
        # the environment listing, so it is lifted out and becomes metadata.component.
        components = [
            c for c in sbom.get("components", [])
            if c.get("name") != "log-foundry" and c.get("name") not in _INSTALLER
        ]
        sbom["components"] = components
        sbom.setdefault("metadata", {})["component"] = {
            "type": "library",
            "name": "log-foundry",
            "version": version,
            "bom-ref": f"log-foundry@{version}",
        }

        names = {c.get("name", "") for c in components}
        if not components:
            _die("the SBOM has zero components; the described environment was empty")
        if leaked := (names & _BUILD_ONLY):
            _die(f"build tooling leaked into the SBOM: {sorted(leaked)}")
        if missing := (_EXPECTED - names):
            _die(f"expected packages absent from the SBOM: {sorted(missing)}")

        args.output.write_text(json.dumps(sbom, indent=2) + "\n")
        # The generator validated what IT wrote; everything above edited that document afterwards.
        # Re-validate the file that will actually be published, using the validator that ships
        # inside cyclonedx-python-lib (a dependency of the tool, so no extra pin).
        _validate(tooling, args.output, str(sbom["specVersion"]))

    print(f"wrote {args.output} — log-foundry {version}, {len(components)} components")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
