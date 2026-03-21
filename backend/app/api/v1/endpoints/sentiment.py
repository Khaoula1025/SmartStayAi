"""
backend/app/api/v1/endpoints/sentiment.py
GET /api/v1/sentiment/summary        — full sentiment summary
GET /api/v1/sentiment/monthly        — monthly aggregation only
GET /api/v1/sentiment/correlation    — correlation with occupancy
GET /api/v1/sentiment/reviews        — notable positive/negative reviews
"""

import json
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Depends, Query
from app.api.deps import get_current_user

sentimentRouter = APIRouter(prefix="/sentiment", tags=["Sentiment"])

SUMMARY_PATH = Path("data/sentiment/sentiment_summary.json")

_cache: dict = {}

def _load_summary() -> dict:
    if "summary" not in _cache:
        if not SUMMARY_PATH.exists():
            return {}
        with open(SUMMARY_PATH) as f:
            _cache["summary"] = json.load(f)
    return _cache["summary"]

NOT_FOUND = ("Sentiment data not found. "
             "Run scripts/13_sentiment_analysis.py first.")


@sentimentRouter.get("/summary")
def get_sentiment_summary(current_user=Depends(get_current_user)):
    """
    Full sentiment summary — use this for the Sentiment dashboard page.
    Returns overall stats, monthly breakdown, correlation, notable reviews.
    """
    data = _load_summary()
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND)
    return data


@sentimentRouter.get("/monthly")
def get_monthly_sentiment(
    date_from: Optional[str] = Query(None),
    date_to:   Optional[str] = Query(None),
    current_user=Depends(get_current_user)
):
    """
    Monthly sentiment aggregation.
    Use for the sentiment trend line chart.
    """
    data = _load_summary()
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND)

    monthly = data.get("monthly", [])
    if date_from:
        monthly = [m for m in monthly if m["year_month"] >= date_from[:7]]
    if date_to:
        monthly = [m for m in monthly if m["year_month"] <= date_to[:7]]

    return {"count": len(monthly), "data": monthly}


@sentimentRouter.get("/correlation")
def get_sentiment_correlation(current_user=Depends(get_current_user)):
    """
    Sentiment vs occupancy correlation results.
    Use for the correlation chart and insight box.
    """
    data = _load_summary()
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND)

    correlation = data.get("correlation", {})
    if not correlation:
        raise HTTPException(
            status_code=404,
            detail="No correlation data. Ensure training_matrix.csv exists and re-run script 13."
        )
    return correlation


@sentimentRouter.get("/reviews")
def get_notable_reviews(
    sentiment: Optional[str] = Query(None, description="'positive' or 'negative'"),
    current_user=Depends(get_current_user)
):
    """
    Notable positive and negative review excerpts.
    Use for the review cards on the Sentiment page.
    """
    data = _load_summary()
    if not data:
        raise HTTPException(status_code=404, detail=NOT_FOUND)

    notable = data.get("notable_reviews", {})

    if sentiment == "positive":
        return {"reviews": notable.get("top_positive", [])}
    if sentiment == "negative":
        return {"reviews": notable.get("top_negative", [])}

    return notable
