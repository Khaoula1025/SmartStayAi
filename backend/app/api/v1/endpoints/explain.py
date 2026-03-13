"""
backend/app/api/v1/endpoints/explain.py
GET /api/v1/explain/{date}        — single date SHAP explanation
GET /api/v1/explain/summary       — global feature importance
GET /api/v1/explain/              — all dates (paginated)
"""

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from app.api.deps import get_current_user

explainRouter = APIRouter(prefix="/explain", tags=["Explain"])

# ── Load SHAP files once at startup ──────────────────────────────────────────
SHAP_DIR = Path("data/shap")

def _load_explanations() -> dict:
    path = SHAP_DIR / "shap_explanations.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)

def _load_summary() -> dict:
    path = SHAP_DIR / "shap_summary.json"
    if not path.exists():
        return {}
    with open(path) as f:
        return json.load(f)

# Cache in module scope (reloaded on server restart)
_explanations: dict = {}
_summary: dict = {}

def get_explanations() -> dict:
    global _explanations
    if not _explanations:
        _explanations = _load_explanations()
    return _explanations

def get_summary() -> dict:
    global _summary
    if not _summary:
        _summary = _load_summary()
    return _summary


# ── Endpoints ─────────────────────────────────────────────────────────────────

@explainRouter.get("/summary")
def get_shap_summary(current_user=Depends(get_current_user)):
    """
    Global SHAP feature importance across all 307 prediction dates.
    Use this to power the feature importance bar chart on the Analytics page.

    Returns:
      - base_value: mean training occupancy (the model's starting point)
      - feature_importance: list of features sorted by importance (pp)
    """
    summary = get_summary()
    if not summary:
        raise HTTPException(
            status_code=404,
            detail="SHAP summary not found. Run scripts/11_shap_explainability.py first."
        )
    return summary


@explainRouter.get("/{date}")
def get_explanation_by_date(
    date: str,
    current_user=Depends(get_current_user)
):
    """
    SHAP explanation for a single date.
    Use this to power the 'Why this recommendation?' tooltip
    on the Rate Decisions page.

    date format: YYYY-MM-DD (e.g. 2026-04-15)

    Returns:
      - predicted_occ_pct: predicted occupancy %
      - recommended_rate: £ rate
      - rate_tier: promotional/value/standard/high/premium
      - base_value: mean training occupancy (starting point)
      - top_reasons: list of top 3 features driving this prediction
        Each reason has:
          label        — human-readable feature name
          shap_value_pp — impact in percentage points (+/-)
          direction    — "up" or "down"
          feature_value — the actual value for this date
          explanation  — plain English sentence
      - waterfall: cumulative SHAP values (for waterfall chart)
    """
    explanations = get_explanations()
    if not explanations:
        raise HTTPException(
            status_code=404,
            detail="SHAP explanations not found. Run scripts/11_shap_explainability.py first."
        )

    if date not in explanations:
        raise HTTPException(
            status_code=404,
            detail=f"No SHAP explanation found for date {date}. "
                   f"Available range: {min(explanations.keys())} to {max(explanations.keys())}"
        )

    return explanations[date]


@explainRouter.get("/")
def list_explanations(
    date_from: Optional[str] = Query(None, description="Start date YYYY-MM-DD"),
    date_to:   Optional[str] = Query(None, description="End date YYYY-MM-DD"),
    limit:     int           = Query(60,   description="Max results", le=307),
    current_user=Depends(get_current_user)
):
    """
    All SHAP explanations, optionally filtered by date range.
    Returns a summary view (not full waterfall) for listing.
    Use GET /explain/{date} for the full explanation of a specific date.
    """
    explanations = get_explanations()
    if not explanations:
        raise HTTPException(
            status_code=404,
            detail="SHAP explanations not found. Run scripts/11_shap_explainability.py first."
        )

    results = []
    for date, exp in sorted(explanations.items()):
        if date_from and date < date_from:
            continue
        if date_to and date > date_to:
            continue

        # Slim version for listing — top reason only
        top = exp["top_reasons"][0] if exp["top_reasons"] else None
        results.append({
            "date":              exp["date"],
            "predicted_occ_pct": exp["predicted_occ_pct"],
            "recommended_rate":  exp["recommended_rate"],
            "rate_tier":         exp["rate_tier"],
            "top_reason":        top["explanation"] if top else None,
            "top_reason_pp":     top["shap_value_pp"] if top else None,
            "n_reasons":         len(exp["top_reasons"]),
        })
        if len(results) >= limit:
            break

    return {
        "count":   len(results),
        "results": results
    }
