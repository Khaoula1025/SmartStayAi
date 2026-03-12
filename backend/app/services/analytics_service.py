from datetime import date
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.analytics import AccuracyRow, AccuracySummary

HOTEL = 'hickstead'


def get_accuracy_rows(
    db:        Session,
    date_from: Optional[date] = None,
    date_to:   Optional[date] = None,
) -> List[AccuracyRow]:
    sql = text("""
        SELECT
            a.date,
            a.occ_rate                           AS actual_occ,
            p.predicted_occ,
            ABS(a.occ_rate - p.predicted_occ)    AS abs_error,
            (a.occ_rate - p.predicted_occ)        AS signed_error,
            CASE WHEN a.occ_rate BETWEEN p.occ_low AND p.occ_high
                 THEN TRUE ELSE FALSE END          AS within_ci,
            p.rate_tier,
            p.data_quality
        FROM actuals a
        JOIN predictions p ON a.hotel = p.hotel AND a.date = p.date
        JOIN (
            SELECT run_id FROM model_runs
            WHERE promoted = TRUE AND hotel = :hotel
            ORDER BY trained_at DESC LIMIT 1
        ) mr ON p.run_id = mr.run_id
        WHERE a.hotel = :hotel
          AND (:date_from IS NULL OR a.date >= :date_from)
          AND (:date_to   IS NULL OR a.date <= :date_to)
        ORDER BY a.date
    """)

    rows = db.execute(sql, {
        'hotel':     HOTEL,
        'date_from': date_from,
        'date_to':   date_to,
    }).fetchall()

    return [
        AccuracyRow(
            date          = r.date,
            actual_occ    = float(r.actual_occ),
            predicted_occ = float(r.predicted_occ),
            abs_error     = float(r.abs_error),
            signed_error  = float(r.signed_error),
            within_ci     = bool(r.within_ci),
            rate_tier     = r.rate_tier,
            data_quality  = r.data_quality,
        )
        for r in rows
    ]


def get_accuracy_summary(
    db:        Session,
    date_from: Optional[date] = None,
    date_to:   Optional[date] = None,
) -> AccuracySummary:
    rows = get_accuracy_rows(db, date_from, date_to)

    if not rows:
        return AccuracySummary(
            n_dates=0, mean_abs_error=0,
            mean_abs_error_pp=0, within_ci_pct=0,
            by_tier={}, rows=[],
        )

    mean_abs  = sum(r.abs_error for r in rows) / len(rows)
    within_ci = sum(1 for r in rows if r.within_ci) / len(rows) * 100

    by_tier: dict = {}
    for r in rows:
        tier = r.rate_tier or 'unknown'
        if tier not in by_tier:
            by_tier[tier] = {'count': 0, 'total_error': 0.0}
        by_tier[tier]['count']       += 1
        by_tier[tier]['total_error'] += r.abs_error

    for tier in by_tier:
        c = by_tier[tier]['count']
        by_tier[tier]['mean_abs_error_pp'] = round(
            by_tier[tier]['total_error'] / c * 100, 2
        )
        del by_tier[tier]['total_error']

    return AccuracySummary(
        n_dates           = len(rows),
        mean_abs_error    = round(mean_abs, 4),
        mean_abs_error_pp = round(mean_abs * 100, 2),
        within_ci_pct     = round(within_ci, 1),
        by_tier           = by_tier,
        rows              = rows,
    )
