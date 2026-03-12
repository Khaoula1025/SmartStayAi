# Import all models here so SQLAlchemy's Base knows about them
# before create_all() is called in init_db.py
from app.models.user          import User          # noqa
from app.models.prediction    import ModelRun, Prediction  # noqa
from app.models.actual        import Actual        # noqa
from app.models.rate_decision import RateDecision  # noqa
from app.models.pipeline_run  import PipelineRun   # noqa