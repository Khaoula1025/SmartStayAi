from datetime import date
from typing import Optional, List

from sqlalchemy.orm import Session

from app.models.rate_decision import RateDecision
from app.schemas.rate_decision import RateDecisionIn
from app.services.prediction_service import get_prediction_by_date

HOTEL = 'hickstead'


def record_decision(
    db:          Session,
    decision_in: RateDecisionIn,
    username:    str,
) -> RateDecision:
    pred = get_prediction_by_date(db, decision_in.date)
    record = RateDecision(
        hotel            = HOTEL,
        date             = decision_in.date,
        prediction_id    = pred.id if pred else None,
        recommended_rate = decision_in.recommended_rate,
        final_rate       = decision_in.final_rate,
        action           = decision_in.action,
        override_reason  = decision_in.override_reason,
        user_id          = username,
    )
    db.add(record)
    db.commit()
    db.refresh(record)
    return record


def get_decisions(
    db:        Session,
    date_from: Optional[date] = None,
    date_to:   Optional[date] = None,
    action:    Optional[str]  = None,
    limit:     int            = 100,
) -> List[RateDecision]:
    q = db.query(RateDecision).filter(RateDecision.hotel == HOTEL)
    if date_from: q = q.filter(RateDecision.date >= date_from)
    if date_to:   q = q.filter(RateDecision.date <= date_to)
    if action:    q = q.filter(RateDecision.action == action)
    return q.order_by(RateDecision.decided_at.desc()).limit(limit).all()
