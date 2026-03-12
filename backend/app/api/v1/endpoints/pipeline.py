from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.api.deps import get_current_user, require_admin
from app.models.user import User
from app.schemas.pipeline import PipelineStatusOut
from app.services import pipeline_service

pipelineRouter = APIRouter(prefix='/pipeline', tags=['Pipeline'])


@pipelineRouter.post('/trigger')
def trigger_pipeline(
    background_tasks: BackgroundTasks,
    steps:            Optional[str] = None,
    db:               Session       = Depends(get_db),
    current_user:     User          = Depends(require_admin),
):
    """
    Trigger a full ETL pipeline run in the background.
    **Admin role required.**

    Optional: pass `steps=6,7,9` to run specific steps only.
    """
    background_tasks.add_task(pipeline_service.trigger_pipeline_background, steps)
    return {
        'message':      'Pipeline triggered',
        'steps':        steps or 'all (1-10)',
        'triggered_by': current_user.username,
        'timestamp':    datetime.now(timezone.utc).isoformat(),
    }


@pipelineRouter.get('/status', response_model=PipelineStatusOut)
def get_pipeline_status(
    db:           Session = Depends(get_db),
    current_user: User    = Depends(get_current_user),
):
    """Last pipeline run — status, steps completed, rows written."""
    run = pipeline_service.get_latest_run(db)
    if not run:
        raise HTTPException(404, 'No pipeline runs recorded yet')
    return run
