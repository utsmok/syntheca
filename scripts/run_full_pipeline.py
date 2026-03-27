"""Compatibility wrapper for the supported ``syntheca run`` CLI command.

This script remains as a repo-local convenience shim. The preferred supported
surface is the installed CLI:

    syntheca run --output-dir ./output

Running this script forwards its arguments to that same command.
"""

from __future__ import annotations

import sys

from syntheca.cli import main

if __name__ == "__main__":
    try:
        raise SystemExit(main(["run", *sys.argv[1:]]))
    except KeyboardInterrupt:
        print("Aborted by user")
        sys.exit(1)
