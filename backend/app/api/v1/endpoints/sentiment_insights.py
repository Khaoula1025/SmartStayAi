"""
backend/app/api/v1/endpoints/sentiment_insights.py
GET /api/v1/sentiment/insights        — AI-generated improvement report
DELETE /api/v1/sentiment/insights/cache — force regenerate
"""

import json
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends
from app.deps import get_current_user

insightsRouter = APIRouter(prefix="/sentiment", tags=["Sentiment"])

SUMMARY_PATH = Path("data/sentiment/sentiment_summary.json")
CACHE_PATH   = Path("data/sentiment/insights_cache.json")


def _load_summary() -> dict:
    if not SUMMARY_PATH.exists():
        return {}
    with open(SUMMARY_PATH) as f:
        return json.load(f)


def _load_cache() -> dict:
    if not CACHE_PATH.exists():
        return {}
    with open(CACHE_PATH) as f:
        return json.load(f)


def _save_cache(report: dict):
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_PATH, 'w') as f:
        json.dump(report, f, indent=2)


def _build_prompt(summary: dict) -> str:
    overall  = summary.get("overall", {})
    notable  = summary.get("notable_reviews", {})
    monthly  = summary.get("monthly", [])

    positives = notable.get("top_positive", [])
    negatives = notable.get("top_negative", [])

    recent     = [m for m in monthly if m["year_month"] >= "2024-01"]
    worst      = sorted(recent, key=lambda x: x["avg_compound"])[:3]

    pos_text = "\n".join(
        f'{i+1}. [{r["date"]}, {r["rating"]}/5 stars]: "{r["excerpt"]}"'
        for i, r in enumerate(positives)
    )
    neg_text = "\n".join(
        f'{i+1}. [{r["date"]}, {r["rating"]}/5 stars]: "{r["excerpt"]}"'
        for i, r in enumerate(negatives)
    )
    worst_text = "\n".join(
        f'- {m["year_month"]}: sentiment={m["avg_compound"]}, '
        f'{m["pct_negative"]:.0f}% negative, avg stars={m.get("avg_star_rating","N/A")}'
        for m in worst
    )

    return f"""You are a hotel revenue consultant analyzing TripAdvisor reviews for
The Hickstead Hotel in Bolney, West Sussex, England (52-room hotel, UNO Hotels group).

OVERALL STATS:
- Total reviews: {summary.get("total_reviews", 686)}
- Date range: {summary.get("date_range", {}).get("from","2004")} to {summary.get("date_range", {}).get("to","2026")}
- Positive: {overall.get("pct_positive",92.1)}% | Negative: {overall.get("pct_negative",7.6)}%
- Average sentiment score: {overall.get("avg_compound_pct",79.8)}%
- Average star rating: {overall.get("avg_star_rating",3.97)}/5

TOP 5 MOST POSITIVE REVIEWS:
{pos_text}

TOP 5 MOST NEGATIVE REVIEWS:
{neg_text}

WORST RECENT MONTHS (2024 onwards):
{worst_text}

Respond ONLY with valid JSON, no markdown, no backticks, no preamble:

{{
  "overall_assessment": "2-3 sentence summary of overall guest sentiment and key themes",
  "strengths": [
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}},
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}},
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}}
  ],
  "improvement_areas": [
    {{"title": "short title", "priority": "high", "detail": "problem with evidence", "recommendation": "specific action"}},
    {{"title": "short title", "priority": "medium", "detail": "problem with evidence", "recommendation": "specific action"}},
    {{"title": "short title", "priority": "low", "detail": "problem with evidence", "recommendation": "specific action"}}
  ],
  "recent_concern": "1-2 sentences about late 2025 sentiment decline and likely causes",
  "revenue_impact": "1-2 sentences on how fixing these areas could impact occupancy and RevPAR"
}}"""


def _generate_with_gemini(prompt: str) -> dict:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY not set in environment")

    try:
        import google.generativeai as genai
    except ImportError:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install",
                               "google-generativeai", "--quiet"])
        import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-1.5-flash")
    resp  = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(temperature=0.3, max_output_tokens=1200)
    )
    text = resp.text.strip().replace("```json","").replace("```","").strip()
    return json.loads(text)


@insightsRouter.get("/insights")
def get_sentiment_insights(
    force: bool = False,
    current_user=Depends(get_current_user)
):
    """
    AI-generated hotel improvement report (Google Gemini 1.5 Flash — free tier).
    Cached to data/sentiment/insights_cache.json after first generation.
    Pass ?force=true to regenerate.
    """
    if not force:
        cached = _load_cache()
        if cached:
            cached["from_cache"] = True
            return cached

    summary = _load_summary()
    if not summary:
        raise HTTPException(status_code=404,
            detail="Run scripts/13_sentiment_analysis.py first.")

    try:
        report = _generate_with_gemini(_build_prompt(summary))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON from Gemini: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Generation failed: {e}")

    import datetime
    report["generated_at"] = datetime.datetime.now().isoformat()
    report["from_cache"]   = False
    report["model"]        = "gemini-1.5-flash"
    _save_cache(report)
    return report


@insightsRouter.delete("/insights/cache")
def clear_insights_cache(current_user=Depends(get_current_user)):
    """Clear cached report so next GET regenerates it."""
    if CACHE_PATH.exists():
        CACHE_PATH.unlink()
        return {"message": "Cache cleared. Next GET /insights will regenerate."}
    return {"message": "No cache found."}
