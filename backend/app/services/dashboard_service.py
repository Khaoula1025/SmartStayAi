from datetime import date, datetime, timezone, timedelta
from typing import List

from sqlalchemy.orm import Session

from app.models.prediction import ModelRun, Prediction
from app.schemas.dashboard import (
    DashboardSummary, OccupancyKpi, PaceKpi,
    ModelKpi, UpcomingHighlight,
)
from app.services.prediction_service import get_latest_run_id

HOTEL = 'hickstead'


def get_dashboard_summary(db: Session) -> DashboardSummary:
    today    = date.today()
    alerts   = []

    # ── Latest model run ──────────────────────────────────────────────────────
    latest_run = (
        db.query(ModelRun)
        .filter(ModelRun.promoted == True, ModelRun.hotel == HOTEL)
        .order_by(ModelRun.trained_at.desc())
        .first()
    )

    model_kpi = ModelKpi(
        mae_operational  = float(latest_run.mae_operational)  if latest_run and latest_run.mae_operational  else None,
        occ_accuracy_pct = float(latest_run.occ_accuracy_pct) if latest_run and latest_run.occ_accuracy_pct else None,
        model_type       = latest_run.model_type if latest_run else None,
        last_trained     = latest_run.trained_at.isoformat() if latest_run else None,
    )

    # ── Predictions for next 90 days ──────────────────────────────────────────
    run_id = get_latest_run_id(db)
    preds  = []

    if run_id:
        preds = (
            db.query(Prediction)
            .filter(
                Prediction.hotel  == HOTEL,
                Prediction.run_id == run_id,
                Prediction.date   >= today,
                Prediction.date   <= today + timedelta(days=90),
            )
            .order_by(Prediction.date)
            .all()
        )
    else:
        alerts.append('No promoted model run found — predictions unavailable')

    # ── Occupancy KPIs ────────────────────────────────────────────────────────
    def avg_occ(days: int) -> float:
        cutoff = today + timedelta(days=days)
        subset = [p for p in preds if p.date <= cutoff]
        if not subset:
            return 0.0
        return round(sum(float(p.predicted_occ) for p in subset) / len(subset), 4)

    occ_kpi = OccupancyKpi(
        next_30d_avg = avg_occ(30),
        next_60d_avg = avg_occ(60),
        next_90d_avg = avg_occ(90),
    )

    # ── Pace KPIs ─────────────────────────────────────────────────────────────
    pace_gaps = [
        int(p.pace_gap) for p in preds
        if p.pace_gap is not None and p.date <= today + timedelta(days=60)
    ]

    pace_kpi = PaceKpi(
        avg_pace_gap = round(sum(pace_gaps) / len(pace_gaps), 1) if pace_gaps else 0.0,
        dates_ahead  = sum(1 for g in pace_gaps if g > 0),
        dates_behind = sum(1 for g in pace_gaps if g < 0),
    )

    # ── BOB data quality ──────────────────────────────────────────────────────
    # Infer from the most recent predictions' data_quality flags
    recent_preds = [p for p in preds if p.date <= today + timedelta(days=30)]
    if recent_preds:
        qualities   = [p.data_quality for p in recent_preds if p.data_quality]
        bob_quality = 'high' if qualities.count('high') > len(qualities) / 2 \
                      else 'medium' if 'medium' in qualities \
                      else 'low'
    else:
        bob_quality = 'low'

    if bob_quality == 'low':
        alerts.append('BOB pickup data is stale or missing — predictions use calendar features only')

    # ── Highlights: top 5 dates worth flagging ────────────────────────────────
    highlights: List[UpcomingHighlight] = []
    for p in preds[:60]:   # next 60 days only
        flag = None
        if p.is_local_event:
            flag = 'event'
        elif p.is_bank_holiday:
            flag = 'bank_holiday'
        elif float(p.predicted_occ) >= 0.85:
            flag = 'high_demand'
        elif p.data_quality == 'low':
            flag = 'low_quality'

        if flag:
            highlights.append(UpcomingHighlight(
                date             = p.date,
                predicted_occ    = round(float(p.predicted_occ), 4),
                recommended_rate = round(float(p.recommended_rate), 2),
                rate_tier        = p.rate_tier or 'standard',
                flag             = flag,
            ))

    # Sort: events first, then high demand, limit to 8
    flag_order = {'event': 0, 'bank_holiday': 1, 'high_demand': 2, 'low_quality': 3}
    highlights.sort(key=lambda h: (flag_order.get(h.flag, 9), h.date))
    highlights = highlights[:8]

    return DashboardSummary(
        hotel       = HOTEL,
        as_of       = datetime.now(timezone.utc).isoformat(),
        bob_quality = bob_quality,
        occupancy   = occ_kpi,
        pace        = pace_kpi,
        model       = model_kpi,
        highlights  = highlights,
        alerts      = alerts,
    )
