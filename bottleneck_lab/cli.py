"""Diagnose ANY Spark event log.

    python -m bottleneck_lab.cli /path/to/eventlog            # markdown report
    python -m bottleneck_lab.cli app-2026.log.gz --json       # machine-readable
    python -m bottleneck_lab.cli app.log --target-slots 64    # project onto a bigger cluster

Where to get an event log: enable spark.eventLog.enabled=true (+ spark.eventLog.dir), or
download one from the Spark History Server ("Download" link on an application) or from
your platform's cluster logs. Both plain and .gz files work.
"""
import argparse
import json
import sys

from .diagnose import diagnose, render_markdown
from .eventlog import parse_event_log


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("eventlog")
    ap.add_argument("--target-slots", type=int, default=16,
                    help="parallel slots to project onto for the skew 'straggler tax' (default 16)")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of markdown")
    a = ap.parse_args(argv)
    d = diagnose(parse_event_log(a.eventlog), a.target_slots)
    if a.json:
        json.dump(d.to_dict(), sys.stdout, indent=1)
        print()
    else:
        print(render_markdown(d))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
