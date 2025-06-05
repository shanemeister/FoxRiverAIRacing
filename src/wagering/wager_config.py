# src/wagering/config.py
# ── field-size filter ─────────────────────────────
MIN_FIELD       = 5
MAX_FIELD       = 14

# ── gap thresholds driving the rule engine ───────
GAP_KEY_THR     = 0.30        # straight / key
GAP_BOX_THR     = 0.15        # boxing

# ── Kelly / bankroll management ──────────────────
KELLY_THRESHOLD = 0.02        # ignore horse if Kelly < 2 %
KELLY_FRACTION  = 1.0         # full-Kelly stake
MAX_FRACTION    = 0.05        # never risk > 5 % of bankroll on any race
EDGE_MIN        = 0.03        # overlay filter

BANKROLL_START  = 2_000.00    # opening bankroll
TRACK_MIN       = 20.00        # $2 track minimum