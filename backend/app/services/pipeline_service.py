import logging
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session

from app.models.pipeline_run import PipelineRun

HOTEL = 'hickstead'
log   = logging.getLogger('smartstay.pipeline_service')


def get_latest_run(db: Session) -> Optional[PipelineRun]:
    return (
        db.query(PipelineRun)
        .filter(PipelineRun.hotel == HOTEL)
        .order_by(PipelineRun.started_at.desc())
        .first()
    )


def trigger_pipeline_background(steps: Optional[str] = None) -> None:
    """
    Called as a BackgroundTask from the pipeline/trigger endpoint.
    Imports the pipeline at call time so the API starts without ML deps.
    """
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    try:
        from scripts.pipeline import run_pipeline
        step_list = (
            [int(s.strip()) for s in steps.split(',')]
            if steps else None
        )
        log.info(f"Background pipeline started (steps={step_list or 'all'})")
        run_pipeline(steps=step_list, triggered_by='api')
    except Exception as e:
        log.error(f"Background pipeline failed: {e}")
