"""Locked label geometry for all filetrack ZPL (LOG + STATUS).

Physical stock: 2.625\" wide x 1\" tall @ 203 dpi → 532 x 203 dots.
Every rendered label must pass through enforce_label_geometry() before send.
"""
from __future__ import annotations

import re

from filetrack.config import LABEL_HEIGHT_DOTS, LABEL_WIDTH_DOTS


def enforce_label_geometry(zpl: str) -> str:
    """Force every label in `zpl` to LABEL_WIDTH_DOTS x LABEL_HEIGHT_DOTS.

    Rewrites ^PW/^LL and forces ^LS0 (label shift; not length). Ensures ^PQ1
    so the printer does not advance a larger stock size or print quantity > 1
    (common cause of wasted labels).
    """
    w, h = LABEL_WIDTH_DOTS, LABEL_HEIGHT_DOTS
    out = zpl
    out = re.sub(r"\^PW\d+", f"^PW{w}", out)
    out = re.sub(r"\^LL\d+", f"^LL{h}", out)
    # ^LS is label *shift*, not length — keep it at 0 (older templates wrongly
    # used ^LS203 matching height; that pushed content vs the calibrated stock).
    out = re.sub(r"\^LS\d+", "^LS0", out)

    chunks = re.split(r"(?=\^XA)", out)
    fixed: list[str] = []
    for chunk in chunks:
        if not chunk.strip():
            fixed.append(chunk)
            continue
        if "^XA" in chunk[:12] or chunk.lstrip().startswith("^XA"):
            if f"^LL{h}" in chunk and "^LS0" not in chunk:
                chunk = chunk.replace(f"^LL{h}", f"^LL{h}\n^LS0", 1)
            if "^PQ" not in chunk:
                if "^LH0,0" in chunk:
                    chunk = chunk.replace("^LH0,0", "^LH0,0\n^PQ1,0,1,Y", 1)
                elif "^LS0" in chunk:
                    chunk = chunk.replace("^LS0", "^LS0\n^PQ1,0,1,Y", 1)
            else:
                chunk = re.sub(r"\^PQ[^\n\^]*", "^PQ1,0,1,Y", chunk)
        fixed.append(chunk)
    out = "".join(fixed)

    if f"^PW{w}" not in out or f"^LL{h}" not in out:
        raise ValueError(
            f"Label ZPL missing locked geometry ^PW{w}/^LL{h} "
            f"(2.625x1 in @ 203dpi). Refusing to print."
        )
    return out
