"""
backend/app/api/v1/endpoints/seasonality.py
GET /api/v1/analytics/seasonality/yearly      — monthly occupancy effect
GET /api/v1/analytics/seasonality/weekly      — day-of-week occupancy effect
GET /api/v1/analytics/seasonality/trend       — long-term trend
GET /api/v1/analytics/seasonality/comparison  — GBM vs Prophet per date
GET /api/v1/analytics/seasonality/            — all components in one call
"""

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from app.api.deps import get_current_user

seasonalityRouter = APIRouter(prefix="/analytics/seasonality", tags=["Seasonality"])

# ── Load Prophet files once at startup ───────────────────────────────────────
PROPHET_DIR = Path("data/prophet")

def _load_json(filename: str) -> list | dict:
    path = PROPHET_DIR / filename
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)

# Module-level cache
_cache: dict = {}

def _get(key: str, filename: str):
    if key not in _cache:
        data = _load_json(filename)
        if not data:
            return None
        _cache[key] = data
    return _cache[key]

NOT_FOUND_MSG = "Run scripts/12_prophet_seasonality.py to generate this data."


# ── Endpoints ─────────────────────────────────────────────────────────────────

@seasonalityRouter.get("/")
def get_all_seasonality(current_user=Depends(get_current_user)):
    """
    All seasonality components in one call.
    Use this for the Analytics page Seasonality tab to avoid 4 separate requests.

    Returns:
      - yearly:     monthly effect in percentage points
      - weekly:     day-of-week effect in percentage points
      - trend:      long-term trend (weekly resampled)
      - comparison: GBM vs Prophet agreement stats (not full per-date list)
    """
    yearly     = _get("yearly",     "seasonality_yearly.json")
    weekly     = _get("weekly",     "seasonality_weekly.json")
    trend      = _get("trend",      "seasonality_trend.json")
    comparison = _get("comparison", "forecast_comparison.json")

    if not yearly:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)

    # Compute comparison stats summary (not the full 307-row list)
    comparison_stats = None
    if comparison:
        n_agree   = sum(1 for d in comparison if d["models_agree"])
        n_total   = len(comparison)
        avg_gap   = sum(d["agreement_gap_pp"] for d in comparison) / n_total
        high_conf = [d for d in comparison if d["confidence"] == "high"]
        low_conf  = [d for d in comparison if d["confidence"] == "low"]
        comparison_stats = {
            "n_dates":          n_total,
            "n_agree":          n_agree,
            "agreement_pct":    round(n_agree / n_total * 100, 1),
            "avg_gap_pp":       round(avg_gap, 1),
            "high_confidence_dates": len(high_conf),
            "low_confidence_dates":  len(low_conf),
            "interpretation": (
                f"The GBM+RF ensemble and Prophet agree on {n_agree} of {n_total} dates "
                f"({n_agree/n_total*100:.0f}%). On the remaining {n_total-n_agree} dates, "
                f"predictions differ by more than 10pp — these carry higher uncertainty."
            )
        }

    return {
        "yearly":           yearly,
        "weekly":           weekly,
        "trend":            trend,
        "comparison_stats": comparison_stats,
    }


@seasonalityRouter.get("/yearly")
def get_yearly_seasonality(current_user=Depends(get_current_user)):
    """
    Monthly seasonality effect in percentage points.
    Use for a bar chart showing which months are above/below average.

    Each entry:
      month       — 1-12
      month_name  — 'Jan', 'Feb', etc.
      value       — raw Prophet multiplicative factor
      effect_pp   — deviation from average in percentage points
                    positive = above average demand, negative = below
    """
    data = _get("yearly", "seasonality_yearly.json")
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)
    return {
        "data":        data,
        "peak_month":  max(data, key=lambda x: x["effect_pp"])["month_name"],
        "trough_month": min(data, key=lambda x: x["effect_pp"])["month_name"],
        "peak_effect_pp":   round(max(d["effect_pp"] for d in data), 1),
        "trough_effect_pp": round(min(d["effect_pp"] for d in data), 1),
    }


@seasonalityRouter.get("/weekly")
def get_weekly_seasonality(current_user=Depends(get_current_user)):
    """
    Day-of-week seasonality effect in percentage points.
    Use for a bar chart showing weekday vs weekend patterns.

    Each entry:
      dow         — 0=Monday, 6=Sunday
      day_name    — 'Monday', etc.
      day_short   — 'Mon', etc.
      effect_pp   — deviation from average in pp
    """
    data = _get("weekly", "seasonality_weekly.json")
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)
    return {
        "data":       data,
        "best_day":   max(data, key=lambda x: x["effect_pp"])["day_name"],
        "worst_day":  min(data, key=lambda x: x["effect_pp"])["day_name"],
        "weekend_premium_pp": round(
            sum(d["effect_pp"] for d in data if d["dow"] in (5, 6)) / 2 -
            sum(d["effect_pp"] for d in data if d["dow"] in (0, 1, 2, 3, 4)) / 5,
            1
        ),
    }


@seasonalityRouter.get("/trend")
def get_trend(current_user=Depends(get_current_user)):
    """
    Long-term occupancy trend (weekly resampled).
    Shows the hotel ramp-up from opening (Apr 2024) through 2026 forecast.

    Each entry:
      date        — YYYY-MM-DD (weekly)
      trend_pct   — trend component as occupancy %
      is_forecast — True if after training data ends (2025-12-30)
    """
    data = _get("trend", "seasonality_trend.json")
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)

    hist = [d for d in data if not d["is_forecast"]]
    fc   = [d for d in data if d["is_forecast"]]

    return {
        "data": data,
        "historical_start": hist[0]["trend_pct"] if hist else None,
        "historical_end":   hist[-1]["trend_pct"] if hist else None,
        "forecast_end":     fc[-1]["trend_pct"] if fc else None,
        "trend_direction":  "up" if (fc[-1]["trend_pct"] if fc else 0) > (hist[0]["trend_pct"] if hist else 0) else "stable",
        "interpretation": (
            f"Hotel occupancy trend grew from {hist[0]['trend_pct']:.1f}% "
            f"(Apr 2024 opening) to {hist[-1]['trend_pct']:.1f}% (Dec 2025). "
            f"2026 forecast trend: {fc[-1]['trend_pct']:.1f}%."
        ) if hist else "No trend data available."
    }


@seasonalityRouter.get("/comparison")
def get_model_comparison(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
    confidence: Optional[str] = Query(None, description="Filter by 'high' or 'low'"),
    current_user=Depends(get_current_user)
):
    """
    GBM+RF ensemble vs Prophet predictions side by side.
    Use for the dual-line chart on Analytics page.

    Each entry:
      date            — YYYY-MM-DD
      gbm_occ_pct     — GBM+RF prediction %
      prophet_occ_pct — Prophet prediction %
      agreement_gap_pp — |GBM - Prophet| in pp
      models_agree    — True if gap < 10pp
      confidence      — 'high' or 'low'
      rate_tier       — from GBM prediction
    """
    data = _get("comparison", "forecast_comparison.json")
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND_MSG)

    results = data
    if date_from:
        results = [d for d in results if d["date"] >= date_from]
    if date_to:
        results = [d for d in results if d["date"] <= date_to]
    if confidence:
        results = [d for d in results if d["confidence"] == confidence]

    return {
        "count":   len(results),
        "results": results,
    }
