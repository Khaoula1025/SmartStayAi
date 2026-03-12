from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user
from app.models.user import User
from app.schemas.dashboard import DashboardSummary
from app.services import dashboard_service

dashboardRouter = APIRouter(prefix='/dashboard', tags=['Dashboard'])


@dashboardRouter.get('/summary', response_model=DashboardSummary)
def get_dashboard_summary(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """
    Single endpoint that returns everything the React dashboard needs
    on first load: occupancy KPIs, pace, model metrics, highlights, alerts.

    The frontend calls this once — no need for 5 separate requests.
    """
    return dashboard_service.get_dashboard_summary(db)
