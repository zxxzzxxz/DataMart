"""Export the canonical node catalog; CI checks the committed frontend fixture."""

import argparse
import json
from pathlib import Path

from datamart.workflows.catalog import list_node_types

TARGET = Path(__file__).resolve().parents[1] / "designer/frontend/src/generated/node-catalog.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    content = json.dumps(list_node_types(), ensure_ascii=False, indent=2) + "\n"
    if arguments.check:
        if not TARGET.exists() or TARGET.read_text(encoding="utf-8") != content:
            raise SystemExit("Node catalog is stale: run python scripts/export-node-catalog.py")
    else:
        TARGET.parent.mkdir(parents=True, exist_ok=True)
        TARGET.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    main()
