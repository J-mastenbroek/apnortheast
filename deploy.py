#!/usr/bin/env python3
"""Deploy this repo (including .env) to the trading server and build it.

    py deploy.py                                          # upload + cargo build --release
    py deploy.py --run "cargo run --release --example hitter -- <id>"

The ssh target is DEPLOY_HOST in .env, e.g. DEPLOY_HOST=ubuntu@1.2.3.4
"""

import argparse
import io
import subprocess
import sys
import tarfile
from pathlib import Path

REMOTE_DIR = "~/predictfunalpha"
ROOT = Path(__file__).resolve().parent
EXCLUDE = {".git", ".claude", "target", "__pycache__"}


def env(key: str) -> str | None:
    for line in (ROOT / ".env").read_text().splitlines():
        k, _, v = line.partition("=")
        if k.strip() == key:
            return v.split("#")[0].strip() or None
    return None


def bundle() -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path in sorted(ROOT.rglob("*")):
            rel = path.relative_to(ROOT)
            if EXCLUDE.intersection(rel.parts) or path.suffix == ".pem" or not path.is_file():
                continue
            tar.add(path, arcname=rel.as_posix())
    return buf.getvalue()


def ssh(host: str, cmd: str, stdin: bytes | None = None) -> None:
    result = subprocess.run(["ssh", "-o", "BatchMode=yes", host, cmd], input=stdin)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", help="command to run in the repo on the server after building")
    args = parser.parse_args()

    if not (ROOT / ".env").exists():
        sys.exit(".env missing")
    host = env("DEPLOY_HOST") or sys.exit("set DEPLOY_HOST=user@host in .env")

    data = bundle()
    print(f"uploading {len(data) / 1024:.0f} KiB to {host}:{REMOTE_DIR}")
    # Replace crates/ so deleted files disappear too; target/ and sdk-bench/node_modules survive.
    ssh(host, f"mkdir -p {REMOTE_DIR} && rm -rf {REMOTE_DIR}/crates && tar xzf - -C {REMOTE_DIR} && chmod 600 {REMOTE_DIR}/.env", data)

    print("building")
    ssh(host, f"cd {REMOTE_DIR} && ~/.cargo/bin/cargo build --release --examples")

    if args.run:
        ssh(host, f"cd {REMOTE_DIR} && PATH=$HOME/.cargo/bin:$PATH {args.run}")


if __name__ == "__main__":
    main()
