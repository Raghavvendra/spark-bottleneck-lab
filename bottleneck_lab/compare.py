"""Step 4 of the procedure: FIX, then RE-MEASURE.

A fix is only a fix if (a) the answer is unchanged, (b) the clock moved, and
(c) the thing the diagnosis blamed is actually gone. Experts check all three;
beginners check (b) and hope.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

from .diagnose import Diagnosis


@dataclass
class Comparison:
    variant: str
    wall_before_s: float
    wall_after_s: float
    speedup_measured: float
    projected_before_s: float
    projected_after_s: float
    speedup_projected: float
    result_identical: bool
    binding_before: str
    binding_after: str
    resolved: List[str]       # finding kinds present before, absent after
    remaining: List[str]      # present in both
    introduced: List[str]     # absent before, present after
    verdict: str


def _dominant_binding(d: Diagnosis) -> str:
    dom = next((s for s in d.stages if s.stage_id == d.dominant_stage), None)
    return dom.binding_resource if dom else "n/a"


def compare(variant: str, before: Diagnosis, after: Diagnosis,
            wall_before: float, wall_after: float,
            checksum_before: str, checksum_after: str) -> Comparison:
    kb = {f.kind for f in before.findings}
    ka = {f.kind for f in after.findings}
    sm = wall_before / wall_after if wall_after else 0.0
    sp = before.projected_ms / after.projected_ms if after.projected_ms else 0.0
    same = checksum_before == checksum_after
    bb, ba = _dominant_binding(before), _dominant_binding(after)

    # Verdicts use MEASURED wall-clock only. The projected fields are kept in the output but the
    # projection model failed validation against real 4-core runs (see README), so it is never
    # used to decide whether a fix worked.
    if not same:
        verdict = "REJECTED: result changed. A fast wrong answer is not a fix."
    elif sm >= 1.15:
        moved = "the binding resource changed" if bb != ba else "the same resource still binds, with less work"
        verdict = f"CONFIRMED: {sm:.2f}x faster measured, result identical; {moved}."
    elif sm <= 0.95:
        verdict = (f"REGRESSION: {1 / sm:.2f}x SLOWER measured. The fix costs more than the problem "
                   f"at this scale.")
    else:
        verdict = f"NEUTRAL: {sm:.2f}x measured. Not worth the complexity."
    return Comparison(variant, wall_before, wall_after, sm, before.projected_ms / 1000,
                      after.projected_ms / 1000, sp, same, bb, ba,
                      sorted(kb - ka), sorted(kb & ka), sorted(ka - kb), verdict)
