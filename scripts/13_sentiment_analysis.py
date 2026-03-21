"""
Script 13 — Sentiment Analysis
================================
Scrapes TripAdvisor reviews for The Hickstead Hotel using Apify,
runs VADER sentiment scoring, correlates with occupancy data.

Outputs:
  data/sentiment/tripadvisor_reviews_raw.json   — raw scraped reviews
  data/sentiment/reviews_scored.csv             — reviews + VADER scores
  data/sentiment/monthly_sentiment.csv          — aggregated by month
  data/sentiment/sentiment_summary.json         — API-ready summary

Usage:
  uv run scripts/13_sentiment_analysis.py
  uv run scripts/13_sentiment_analysis.py --skip-scrape   # reuse existing raw file
"""

import os, sys, json, time, argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path('backend/.env'))

# ── Paths ─────────────────────────────────────────────────────────────────────
SENTIMENT_DIR  = Path('data/sentiment')
SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

RAW_REVIEWS    = SENTIMENT_DIR / 'tripadvisor_reviews_raw.json'
SCORED_CSV     = SENTIMENT_DIR / 'reviews_scored.csv'
MONTHLY_CSV    = SENTIMENT_DIR / 'monthly_sentiment.csv'
SUMMARY_JSON   = SENTIMENT_DIR / 'sentiment_summary.json'
TRAIN_MATRIX   = Path('data/processed/training_matrix.csv')

# ── Config ────────────────────────────────────────────────────────────────────
APIFY_TOKEN        = os.getenv('APIFY_TOKEN')
TRIPADVISOR_URL    = (
    'https://www.tripadvisor.co.uk/Hotel_Review-g504217-d262352'
    '-Reviews-The_Hickstead-Bolney_West_Sussex_England.html'
)
HOTEL_NAME         = 'The Hickstead Hotel'
MAX_REVIEWS        = 700  # scrape all available (~680)


# ── Step 1: Scrape via Apify ──────────────────────────────────────────────────
def scrape_reviews() -> list:
    """Call Apify TripAdvisor Reviews Scraper and return raw reviews."""
    try:
        from apify_client import ApifyClient
    except ImportError:
        print("Installing apify-client...")
        os.system(f"{sys.executable} -m pip install apify-client --quiet")
        from apify_client import ApifyClient

    if not APIFY_TOKEN:
        sys.exit("ERROR: APIFY_TOKEN not found in environment. Add it to backend/.env")

    print(f"  Connecting to Apify...")
    client = ApifyClient(APIFY_TOKEN)

    print(f"  Starting TripAdvisor scraper for {HOTEL_NAME}...")
    print(f"  URL: {TRIPADVISOR_URL}")
    print(f"  Max reviews: {MAX_REVIEWS}")

    run_input = {
        "startUrls":         [{"url": TRIPADVISOR_URL}],
        "maxReviewsPerQuery": MAX_REVIEWS,
        "reviewRatings":     ["ALL_REVIEW_RATINGS"],
        "reviewsLanguages":  ["en"],
        "scrapeReviewerInfo": False,
    }

    # Run the TripAdvisor Reviews actor
    run = client.actor("maxcopell/tripadvisor-reviews").call(
        run_input=run_input,
        timeout_secs=300,
    )

    print(f"  Scrape complete. Fetching results...")
    reviews = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        reviews.append(item)

    print(f"  Retrieved {len(reviews)} reviews")
    return reviews


def save_raw(reviews: list):
    with open(RAW_REVIEWS, 'w', encoding='utf-8') as f:
        json.dump(reviews, f, indent=2, default=str)
    print(f"  Saved raw reviews: {RAW_REVIEWS}")


def load_raw() -> list:
    with open(RAW_REVIEWS, encoding='utf-8') as f:
        return json.load(f)


# ── Step 2: Parse & normalise reviews ─────────────────────────────────────────
def parse_reviews(raw: list) -> pd.DataFrame:
    """Extract date, rating, text from raw Apify response."""
    rows = []
    for r in raw:
        # Apify TripAdvisor actor field names
        text = (
            r.get('text') or
            r.get('reviewText') or
            r.get('review') or
            r.get('body') or ''
        )
        title = (
            r.get('title') or
            r.get('reviewTitle') or ''
        )
        full_text = f"{title}. {text}".strip('. ')

        rating = (
            r.get('rating') or
            r.get('bubbleRating') or
            r.get('score') or None
        )
        if isinstance(rating, dict):
            rating = rating.get('ratingValue') or rating.get('value')

        date_str = (
            r.get('publishedDate') or
            r.get('date') or
            r.get('reviewDate') or
            r.get('createdAt') or None
        )

        if not full_text or not date_str:
            continue

        try:
            date = pd.to_datetime(date_str, utc=True).tz_localize(None)
        except Exception:
            continue

        rows.append({
            'date':      date,
            'year_month': date.to_period('M'),
            'rating':    float(rating) if rating else None,
            'text':      full_text,
            'text_len':  len(full_text),
        })

    df = pd.DataFrame(rows)
    df = df.sort_values('date').reset_index(drop=True)
    print(f"  Parsed {len(df)} valid reviews")
    print(f"  Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    if 'rating' in df.columns:
        print(f"  Avg star rating: {df['rating'].mean():.2f}")
    return df


# ── Step 3: VADER Sentiment Scoring ───────────────────────────────────────────
def score_sentiment(df: pd.DataFrame) -> pd.DataFrame:
    """Run VADER on each review and add sentiment columns."""
    try:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    except ImportError:
        print("  Installing vaderSentiment...")
        os.system(f"{sys.executable} -m pip install vaderSentiment --quiet")
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

    analyser = SentimentIntensityAnalyzer()

    print(f"  Scoring {len(df)} reviews with VADER...")
    scores = df['text'].apply(lambda t: analyser.polarity_scores(str(t)))

    df['vader_compound'] = scores.apply(lambda s: s['compound'])
    df['vader_pos']      = scores.apply(lambda s: s['pos'])
    df['vader_neg']      = scores.apply(lambda s: s['neg'])
    df['vader_neu']      = scores.apply(lambda s: s['neu'])

    # Classify sentiment
    df['sentiment'] = df['vader_compound'].apply(
        lambda c: 'positive' if c >= 0.05 else ('negative' if c <= -0.05 else 'neutral')
    )

    # Stats
    counts = df['sentiment'].value_counts()
    print(f"  Positive: {counts.get('positive', 0)} | "
          f"Neutral: {counts.get('neutral', 0)} | "
          f"Negative: {counts.get('negative', 0)}")
    print(f"  Avg compound score: {df['vader_compound'].mean():.3f}")

    return df


# ── Step 4: Monthly Aggregation ───────────────────────────────────────────────
def aggregate_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sentiment scores by month."""
    monthly = df.groupby('year_month').agg(
        n_reviews         = ('vader_compound', 'count'),
        avg_compound      = ('vader_compound', 'mean'),
        avg_pos           = ('vader_pos', 'mean'),
        avg_neg           = ('vader_neg', 'mean'),
        pct_positive      = ('sentiment', lambda x: (x == 'positive').mean() * 100),
        pct_negative      = ('sentiment', lambda x: (x == 'negative').mean() * 100),
        pct_neutral       = ('sentiment', lambda x: (x == 'neutral').mean() * 100),
        avg_star_rating   = ('rating', 'mean'),
    ).reset_index()

    monthly['year_month_str'] = monthly['year_month'].astype(str)
    monthly['date']           = pd.to_datetime(monthly['year_month_str'])

    # Round for readability
    for col in ['avg_compound', 'avg_pos', 'avg_neg', 'pct_positive',
                'pct_negative', 'pct_neutral', 'avg_star_rating']:
        monthly[col] = monthly[col].round(3)

    print(f"  Monthly aggregation: {len(monthly)} months")
    return monthly


# ── Step 5: Correlation with Occupancy ────────────────────────────────────────
def correlate_with_occupancy(monthly: pd.DataFrame) -> dict:
    """
    Join monthly sentiment with actual occupancy from training matrix.
    Compute:
      - Same-month correlation (sentiment → occupancy this month)
      - Lag-1 correlation (sentiment → occupancy next month)
    """
    if not TRAIN_MATRIX.exists():
        print("  WARNING: training_matrix.csv not found — skipping correlation")
        return {}

    tm = pd.read_csv(TRAIN_MATRIX, parse_dates=['date'])
    tm['year_month'] = tm['date'].dt.to_period('M')

    # Monthly avg occupancy
    occ_monthly = tm.groupby('year_month').agg(
        avg_occ = ('occ_rate', 'mean')
    ).reset_index()
    occ_monthly['avg_occ_pct'] = (occ_monthly['avg_occ'] * 100).round(1)

    # Merge same-month
    merged = monthly.merge(
        occ_monthly[['year_month', 'avg_occ_pct']],
        on='year_month', how='inner'
    )

    if len(merged) < 3:
        print("  WARNING: Not enough overlapping months for correlation")
        return {}

    # Same-month correlation
    r_same = merged['avg_compound'].corr(merged['avg_occ_pct'])

    # Lag-1: sentiment in month M vs occupancy in month M+1
    merged_lag = merged.copy()
    merged_lag['occ_next_month'] = merged_lag['avg_occ_pct'].shift(-1)
    merged_lag = merged_lag.dropna(subset=['occ_next_month'])
    r_lag1 = merged_lag['avg_compound'].corr(merged_lag['occ_next_month'])

    # Lag-2: sentiment in month M vs occupancy in month M+2
    merged_lag2 = merged.copy()
    merged_lag2['occ_2months'] = merged_lag2['avg_occ_pct'].shift(-2)
    merged_lag2 = merged_lag2.dropna(subset=['occ_2months'])
    r_lag2 = merged_lag2['avg_compound'].corr(merged_lag2['occ_2months'])

    print(f"\n  Correlation Results:")
    print(f"    Same month (sentiment → occ):       r = {r_same:.3f}")
    print(f"    Lag 1 month (sentiment → occ M+1):  r = {r_lag1:.3f}")
    print(f"    Lag 2 months (sentiment → occ M+2): r = {r_lag2:.3f}")

    # Interpret
    def interpret(r):
        a = abs(r)
        if a > 0.7:   return "strong"
        if a > 0.4:   return "moderate"
        if a > 0.2:   return "weak"
        return "negligible"

    # Save merged for API
    merged_out = merged[['year_month', 'avg_compound', 'pct_negative',
                          'n_reviews', 'avg_occ_pct']].copy()
    merged_out['year_month'] = merged_out['year_month'].astype(str)
    merged_out.to_csv(SENTIMENT_DIR / 'sentiment_occupancy_correlation.csv', index=False)

    return {
        'r_same_month':  round(r_same, 3),
        'r_lag_1_month': round(r_lag1, 3),
        'r_lag_2_months': round(r_lag2, 3),
        'n_overlapping_months': len(merged),
        'strength_same':  interpret(r_same),
        'strength_lag1':  interpret(r_lag1),
        'best_lag': (
            'same month' if abs(r_same) >= max(abs(r_lag1), abs(r_lag2))
            else ('1 month lag' if abs(r_lag1) >= abs(r_lag2) else '2 month lag')
        ),
        'interpretation': (
            f"Sentiment has a {interpret(r_lag1)} lagged correlation with occupancy "
            f"(r={r_lag1:.2f} at 1-month lag). "
            f"{'Negative reviews precede lower occupancy the following month.' if r_lag1 > 0.2 else 'No strong predictive relationship detected.'}"
        )
    }


# ── Step 6: Top/Bottom Reviews ────────────────────────────────────────────────
def extract_notable_reviews(df: pd.DataFrame) -> dict:
    """Extract top 5 positive and top 5 negative reviews for display."""
    df_sorted = df.sort_values('vader_compound')

    def clean(text, max_len=200):
        t = str(text).replace('\n', ' ').strip()
        return t[:max_len] + '...' if len(t) > max_len else t

    top_positive = df_sorted.tail(5)[['date', 'vader_compound', 'rating', 'text']].copy()
    top_negative = df_sorted.head(5)[['date', 'vader_compound', 'rating', 'text']].copy()

    def to_list(subset):
        return [
            {
                'date':     row['date'].strftime('%Y-%m-%d'),
                'score':    round(row['vader_compound'], 3),
                'rating':   row['rating'],
                'excerpt':  clean(row['text']),
            }
            for _, row in subset.iterrows()
        ]

    return {
        'top_positive': to_list(top_positive),
        'top_negative': to_list(top_negative),
    }


# ── Step 7: Build Summary JSON ────────────────────────────────────────────────
def build_summary(df: pd.DataFrame, monthly: pd.DataFrame,
                  correlation: dict, notable: dict) -> dict:
    """Build the full summary JSON for the API endpoint."""

    monthly_list = []
    for _, row in monthly.iterrows():
        monthly_list.append({
            'year_month':      str(row['year_month']),
            'date':            row['date'].strftime('%Y-%m-%d'),
            'n_reviews':       int(row['n_reviews']),
            'avg_compound':    float(row['avg_compound']),
            'avg_compound_pct': round(float(row['avg_compound']) * 100, 1),
            'pct_positive':    float(row['pct_positive']),
            'pct_negative':    float(row['pct_negative']),
            'pct_neutral':     float(row['pct_neutral']),
            'avg_star_rating': float(row['avg_star_rating']) if pd.notna(row['avg_star_rating']) else None,
        })

    sentiment_counts = df['sentiment'].value_counts()
    summary = {
        'hotel':              'hickstead',
        'hotel_name':         HOTEL_NAME,
        'source':             'TripAdvisor',
        'generated_at':       datetime.now().isoformat(),
        'total_reviews':      int(len(df)),
        'date_range': {
            'from': df['date'].min().strftime('%Y-%m-%d'),
            'to':   df['date'].max().strftime('%Y-%m-%d'),
        },
        'overall': {
            'avg_compound':       round(float(df['vader_compound'].mean()), 3),
            'avg_compound_pct':   round(float(df['vader_compound'].mean()) * 100, 1),
            'avg_star_rating':    round(float(df['rating'].mean()), 2) if df['rating'].notna().any() else None,
            'pct_positive':       round(float((df['sentiment'] == 'positive').mean() * 100), 1),
            'pct_negative':       round(float((df['sentiment'] == 'negative').mean() * 100), 1),
            'pct_neutral':        round(float((df['sentiment'] == 'neutral').mean() * 100), 1),
            'n_positive':         int(sentiment_counts.get('positive', 0)),
            'n_negative':         int(sentiment_counts.get('negative', 0)),
            'n_neutral':          int(sentiment_counts.get('neutral', 0)),
        },
        'monthly':            monthly_list,
        'correlation':        correlation,
        'notable_reviews':    notable,
    }
    return summary


# ── Main ──────────────────────────────────────────────────────────────────────
def main(skip_scrape: bool = False):
    print("=" * 60)
    print("Script 13 — Sentiment Analysis")
    print("=" * 60)

    # Step 1: Scrape
    print("\n[1/6] Scraping TripAdvisor reviews...")
    if skip_scrape and RAW_REVIEWS.exists():
        print(f"  Skipping scrape — loading existing: {RAW_REVIEWS}")
        raw = load_raw()
        print(f"  Loaded {len(raw)} raw reviews")
    else:
        raw = scrape_reviews()
        save_raw(raw)

    # Step 2: Parse
    print("\n[2/6] Parsing reviews...")
    df = parse_reviews(raw)

    if len(df) == 0:
        sys.exit("ERROR: No reviews could be parsed. Check raw JSON structure.")

    # Step 3: Score
    print("\n[3/6] Running VADER sentiment scoring...")
    df = score_sentiment(df)
    df.to_csv(SCORED_CSV, index=False)
    print(f"  Saved: {SCORED_CSV}")

    # Step 4: Aggregate
    print("\n[4/6] Aggregating by month...")
    monthly = aggregate_monthly(df)
    monthly.to_csv(MONTHLY_CSV, index=False)
    print(f"  Saved: {MONTHLY_CSV}")

    # Step 5: Correlate
    print("\n[5/6] Correlating with occupancy data...")
    correlation = correlate_with_occupancy(monthly)

    # Step 6: Notable reviews + summary
    print("\n[6/6] Building summary...")
    notable = extract_notable_reviews(df)
    summary = build_summary(df, monthly, correlation, notable)

    with open(SUMMARY_JSON, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Saved: {SUMMARY_JSON}")

    # ── Print results ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("SENTIMENT ANALYSIS RESULTS")
    print("=" * 60)
    print(f"  Total reviews scored:  {summary['total_reviews']}")
    print(f"  Date range:            {summary['date_range']['from']} → {summary['date_range']['to']}")
    print(f"  Avg compound score:    {summary['overall']['avg_compound']} "
          f"({summary['overall']['avg_compound_pct']}%)")
    print(f"  Positive:              {summary['overall']['pct_positive']}%")
    print(f"  Negative:              {summary['overall']['pct_negative']}%")
    if summary['overall']['avg_star_rating']:
        print(f"  Avg star rating:       {summary['overall']['avg_star_rating']}/5")

    if correlation:
        print(f"\n  Correlation with occupancy:")
        print(f"    Same month:   r = {correlation['r_same_month']} ({correlation['strength_same']})")
        print(f"    1-month lag:  r = {correlation['r_lag_1_month']} ({correlation['strength_lag1']})")
        print(f"    Best lag:     {correlation['best_lag']}")
        print(f"\n  Interpretation:")
        print(f"    {correlation['interpretation']}")

    print(f"\n✅ Sentiment analysis complete.")
    print(f"   Files in: data/sentiment/")
    print(f"\nNext step: add GET /api/v1/sentiment/summary endpoint to FastAPI")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='SmartStay Sentiment Analysis')
    parser.add_argument('--skip-scrape', action='store_true',
                        help='Skip Apify scrape and use existing raw JSON file')
    args = parser.parse_args()
    main(skip_scrape=args.skip_scrape)
