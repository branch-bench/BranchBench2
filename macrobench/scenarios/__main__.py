"""List and describe the registered scenarios.

    python -m macrobench.scenarios --list              # keys, one per line, in order
    python -m macrobench.scenarios --list --examples   # also the examples (the template)
    python -m macrobench.scenarios --describe [key]    # parameters with types, defaults, docs

The run scripts read ``--list`` instead of keeping their own scenario lists.
"""

import argparse
import sys

from macrobench.scenarios import SCENARIOS, listed_keys
from macrobench.scenarios.params import describe


def _describe(key: str) -> str:
    cls = SCENARIOS[key]
    lines = [f"{key}: {cls.name} ({cls.group}-heavy){'  [example]' if cls.example else ''}"]
    rows = describe(cls.Params)
    if not rows:
        lines.append("  (no parameters)")
    for name, typ, default, meta in rows:
        tags = []
        if meta.get("intensity"):
            tags.append(f"x {meta['intensity']} intensity" + (f", cap {meta['cap']}" if meta.get("cap") else ""))
        if meta.get("choices"):
            tags.append("one of " + "|".join(meta["choices"]))
        tag = f"  [{'; '.join(tags)}]" if tags else ""
        lines.append(f"  {name}: {typ} = {default!r}{tag}")
        if meta.get("doc"):
            lines.append(f"      {meta['doc']}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m macrobench.scenarios", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true", help="print the scenario keys, one per line")
    ap.add_argument("--examples", action="store_true", help="include example scenarios")
    ap.add_argument("--describe", nargs="?", const="", metavar="KEY",
                    help="print a scenario's parameters (all scenarios without KEY)")
    a = ap.parse_args(argv)
    keys = listed_keys(include_examples=a.examples or bool(a.describe))
    if a.describe is not None:
        if a.describe and a.describe not in SCENARIOS:
            print(f"unknown scenario {a.describe!r}; known: {', '.join(keys)}", file=sys.stderr)
            return 1
        print("\n\n".join(_describe(k) for k in ([a.describe] if a.describe else keys)))
        return 0
    print("\n".join(keys))
    return 0


if __name__ == "__main__":
    sys.exit(main())
