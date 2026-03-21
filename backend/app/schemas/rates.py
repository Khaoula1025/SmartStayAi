from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.deps import get_current_user
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
    record = rate_service.record_decision(db, decision, current_user.username)
    return record


@ratesRouter.get('/decisions', response_model=List[RateDecisionOut])
def get_rate_decisions(
    date_from:    Optional[date] = None,
    date_to:      Optional[date] = None,
    action:       Optional[str]  = None,
    limit:        int            = 100,
    db:           Session        = Depends(get_db),
    current_user: User           = Depends(get_current_user),
):
    return rate_service.get_decisions(db, date_from, date_to, action, limit)
