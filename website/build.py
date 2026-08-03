#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the Agent World marketing site")
    parser.add_argument("--output", type=Path, default=Path("_site"))
    args = parser.parse_args()

    source = Path(__file__).resolve().parent
    args.output.mkdir(parents=True, exist_ok=True)

    html = "".join(path.read_text(encoding="utf-8") for path in sorted((source / "fragments").glob("*.html")))
    css = "".join(path.read_text(encoding="utf-8") for path in sorted((source / "styles").glob("*.css")))
    js = (source / "site.js").read_text(encoding="utf-8")

    (args.output / "index.html").write_text(html, encoding="utf-8")
    (args.output / "site.css").write_text(css, encoding="utf-8")
    (args.output / "site.js").write_text(js, encoding="utf-8")


if __name__ == "__main__":
    main()
