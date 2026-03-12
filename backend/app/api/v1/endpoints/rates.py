from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.schemas.rate_decision import RateDecisionIn, RateDecisionOut
from app.services import rate_service

ratesRouter = APIRouter(prefix='/rates', tags=['Rates'])


@ratesRouter.post('/decide', response_model=RateDecisionOut, status_code=201)
def post_rate_decision(
    decision:     RateDecisionIn,
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """
    Record a rate decision for a date.

    - **accept**: use the recommended rate as-is
    - **override**: set a different rate (include final_rate + override_reason)
    - **ignore**: acknowledge but take no action
    """
    record = rate_service.record_decision(db, decision, current_user.username)
    return RateDecisionOut(
        id               = str(record.id),
        date             = record.date,
        recommended_rate = float(record.recommended_rate),
        final_rate       = float(record.final_rate) if record.final_rate else None,
        action           = record.action,
        override_reason  = record.override_reason,
        user_id          = record.user_id,
        decided_at       = record.decided_at,
    )


@ratesRouter.get('/decisions', response_model=List[RateDecisionOut])
def get_rate_decisions(
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    action:       Optional[str]  = None,
    limit:        int            = 100,
    db:           Session        = Depends(get_db),
    current_user: User           = Depends(get_current_user),
):
    """Full audit log of rate decisions."""
    records = rate_service.get_decisions(db, date_from, date_to, action, limit)
    return [
        RateDecisionOut(
            id               = str(r.id),
            date             = r.date,
            recommended_rate = float(r.recommended_rate),
            final_rate       = float(r.final_rate) if r.final_rate else None,
            action           = r.action,
            override_reason  = r.override_reason,
            user_id          = r.user_id,
            decided_at       = r.decided_at,
        )
        for r in records
    ]
