"""Assert built wheel runtime Requires-Dist is exactly websockets/httpx/pydantic."""

from __future__ import annotations

import glob
import re
import sys
import zipfile


def main() -> int:
    wheels = sorted(glob.glob("dist/*.whl"))
    if not wheels:
        print("no wheel in dist/", file=sys.stderr)
        return 1
    wheel = wheels[-1]
    zf = zipfile.ZipFile(wheel)
    meta_name = next(n for n in zf.namelist() if n.endswith("METADATA"))
    meta = zf.read(meta_name).decode()
    deps: set[str] = set()
    for line in meta.splitlines():
        if not line.startswith("Requires-Dist:"):
            continue
        if "extra ==" in line:
            continue
        rest = line.split(":", 1)[1].strip()
        deps.add(re.split(r"[><=!~; ]", rest)[0])
    expected = {"websockets", "httpx", "pydantic"}
    if deps != expected:
        print(f"expected {expected}, got {deps}", file=sys.stderr)
        return 1
    print("OK", deps)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
