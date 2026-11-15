"""Download the pinned Spark jars from Maven Central and verify their checksums.

The Docker image and the CI workflow both call this script, so the jar versions
are declared in exactly one place. It only uses the standard library because it
runs before any project dependency is installed.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

MAVEN_CENTRAL = "https://repo1.maven.org/maven2"


@dataclass(frozen=True)
class Jar:
    """One Maven artifact, pinned by version and SHA-1."""

    group: str
    artifact: str
    version: str
    sha1: str

    @property
    def file_name(self) -> str:
        return f"{self.artifact}-{self.version}.jar"

    @property
    def url(self) -> str:
        group_path = self.group.replace(".", "/")
        return f"{MAVEN_CENTRAL}/{group_path}/{self.artifact}/{self.version}/{self.file_name}"


# Spark 3.5.9 is built against Hadoop 3.3.4, and hadoop-aws 3.3.4 is built
# against AWS SDK bundle 1.12.262. Changing one of the three means changing all.
JARS = (
    Jar(
        "org.apache.iceberg",
        "iceberg-spark-runtime-3.5_2.12",
        "1.10.2",
        "65f3964fe59e1168efad89e39dc9f9be2ce581b7",
    ),
    Jar("org.apache.hadoop", "hadoop-aws", "3.3.4", "a65839fbf1869f81a1632e09f415e586922e4f80"),
    Jar(
        "com.amazonaws",
        "aws-java-sdk-bundle",
        "1.12.262",
        "02deec3a0ad83d13d032b1812421b23d7a961eea",
    ),
    Jar("org.postgresql", "postgresql", "42.7.14", "0e7c94ce80cb1dd9875ff44ffe6a40a8aaa3eefd"),
)


def sha1_of(path: Path) -> str:
    """Return the SHA-1 hex digest of a file, read in chunks."""
    digest = hashlib.sha1(usedforsecurity=False)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def fetch(jar: Jar, dest: Path) -> None:
    """Download one jar unless a verified copy is already present."""
    target = dest / jar.file_name
    if target.exists() and sha1_of(target) == jar.sha1:
        print(f"ok       {jar.file_name}")
        return

    partial = target.with_suffix(".part")
    print(f"download {jar.url}")
    urllib.request.urlretrieve(jar.url, partial)
    actual = sha1_of(partial)
    if actual != jar.sha1:
        partial.unlink()
        raise RuntimeError(
            f"Checksum mismatch for {jar.file_name}: expected {jar.sha1}, got {actual}"
        )
    partial.replace(target)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, default=Path("jars"), help="target directory")
    args = parser.parse_args()

    args.dest.mkdir(parents=True, exist_ok=True)
    for jar in JARS:
        fetch(jar, args.dest)
    return 0


if __name__ == "__main__":
    sys.exit(main())
