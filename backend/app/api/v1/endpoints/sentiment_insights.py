import json
import datetime
import os
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Depends
from app.api.deps import get_current_user

insightsRouter = APIRouter(prefix="/sentiment", tags=["Sentiment"])

SUMMARY_PATH = Path("data/sentiment/sentiment_summary.json")
CACHE_PATH = Path("data/sentiment/insights_cache.json")


# ── File helpers ─────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def _save_cache(report: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(report, indent=2))


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(summary: dict) -> str:
    overall = summary.get("overall", {})
    notable = summary.get("notable_reviews", {})
    monthly = summary.get("monthly", [])

    positives = notable.get("top_positive", [])
    negatives = notable.get("top_negative", [])

    recent_worst = sorted(
        [m for m in monthly if m["year_month"] >= "2024-01"],
        key=lambda m: m["avg_compound"]
    )[:3]

    def fmt_reviews(reviews):
        return "\n".join(
            f'{i+1}. [{r["date"]}, {r["rating"]}/5 stars]: "{r["excerpt"]}"'
            for i, r in enumerate(reviews)
        )

    def fmt_months(months):
        return "\n".join(
            f'- {m["year_month"]}: sentiment={m["avg_compound"]}, '
            f'{m["pct_negative"]:.0f}% negative, avg stars={m.get("avg_star_rating", "N/A")}'
            for m in months
        )

    date_range = summary.get("date_range", {})

    return f"""You are a hotel revenue consultant analyzing TripAdvisor reviews for \
The Hickstead Hotel in Bolney, West Sussex, England (52-room hotel, UNO Hotels group).

OVERALL STATS:
- Total reviews: {summary.get("total_reviews", 686)}
- Date range: {date_range.get("from", "2004")} to {date_range.get("to", "2026")}
- Positive: {overall.get("pct_positive", 92.1)}% | Negative: {overall.get("pct_negative", 7.6)}%
- Average sentiment score: {overall.get("avg_compound_pct", 79.8)}%
- Average star rating: {overall.get("avg_star_rating", 3.97)}/5

TOP 5 MOST POSITIVE REVIEWS:
{fmt_reviews(positives)}

TOP 5 MOST NEGATIVE REVIEWS:
{fmt_reviews(negatives)}

WORST RECENT MONTHS (2024 onwards):
{fmt_months(recent_worst)}

Respond ONLY with valid JSON, no markdown, no backticks, no preamble:

{{
  "overall_assessment": "2-3 sentence summary of overall guest sentiment and key themes",
  "strengths": [
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}},
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}},
    {{"title": "short title", "detail": "1-2 sentence explanation with evidence from reviews"}}
  ],
  "improvement_areas": [
    {{"title": "short title", "priority": "high",   "detail": "problem with evidence", "recommendation": "specific action"}},
    {{"title": "short title", "priority": "medium", "detail": "problem with evidence", "recommendation": "specific action"}},
    {{"title": "short title", "priority": "low",    "detail": "problem with evidence", "recommendation": "specific action"}}
  ],
  "recent_concern": "1-2 sentences about late 2025 sentiment decline and likely causes",
  "revenue_impact": "1-2 sentences on how fixing these areas could impact occupancy and RevPAR"
}}"""


# ── Gemini call ───────────────────────────────────────────────────────────────

def _generate_with_gemini(prompt: str) -> dict:
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        raise ValueError("GOOGLE_API_KEY is not set in the environment")

    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.5-flash")

    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            temperature=0.2,
            max_output_tokens=2000,
            response_mime_type="application/json",
        ),
    )

    raw = response.text.strip()
    cleaned = re.sub(r"^```json\s*|```$", "", raw, flags=re.MULTILINE).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"Gemini returned invalid JSON. Raw output:\n{raw}", cleaned, 0)


# ── Routes ────────────────────────────────────────────────────────────────────

@insightsRouter.get("/insights")
def get_sentiment_insights(
    force: bool = False,
    current_user=Depends(get_current_user),
):
    """
    Returns an AI-generated hotel improvement report (Gemini 2.5 Flash).
    The result is cached in `data/sentiment/insights_cache.json`.
    Pass `?force=true` to bypass the cache and regenerate.
    """
    if not force:
        cached = _load_json(CACHE_PATH)
        if cached:
            return {**cached, "from_cache": True}

    summary = _load_json(SUMMARY_PATH)
    if not summary:
        raise HTTPException(
            status_code=404,
            detail="Summary not found. Run scripts/13_sentiment_analysis.py first.",
        )

    try:
        report = _generate_with_gemini(_build_prompt(summary))
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON from Gemini: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Generation failed: {e}")

    report.update({
        "generated_at": datetime.datetime.now().isoformat(),
        "from_cache": False,
        "model": "gemini-2.5-flash",
    })
    _save_cache(report)
    return report


@insightsRouter.delete("/insights/cache")
def clear_insights_cache(current_user=Depends(get_current_user)):
    """Deletes the cached report so the next GET will regenerate it."""
    if CACHE_PATH.exists():
        CACHE_PATH.unlink()
        return {"message": "Cache cleared. Next GET /insights will regenerate."}
    return {"message": "No cache found."}