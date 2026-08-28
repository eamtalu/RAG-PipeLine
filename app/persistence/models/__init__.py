from app.persistence.models.consumer_cursor import ConsumerCursor  # noqa: F401
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry  # noqa: F401
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry  # noqa: F401
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact  # noqa: F401
from app.persistence.models.analytics_ml import (AnalyticsFeatureSet,  # noqa: F401
                                                AnalyticsPrediction)
from app.persistence.models.log_open_stream import (LogOpenStream,  # noqa: F401
                                                    LogPendingRequest)
from app.config.database import Base
from app.persistence.models.job import Job
from app.persistence.models.chunk import Chunk
from app.persistence.models.ChunkEntity import ChunkEntity
from app.persistence.models.embedding_queue import EmbeddingQueueItem
from app.persistence.models.log_transaction import LogTransaction
from app.persistence.models.log_entry import LogEntry
from app.persistence.models.log_entry_assignment import LogEntryAssignment
from app.persistence.models.log_regroup_pending import LogRegroupPending
from app.persistence.models.log_regroup_run import LogRegroupRun
from app.persistence.models.log_stream_frontier import LogStreamFrontier
from app.persistence.models.log_ssh_source import LogSshSource
from app.persistence.models.log_ssh_file_checkpoint import LogSshFileCheckpoint
from app.persistence.models.log_ssh_fetch_run import LogSshFetchRun
from app.persistence.models.log_source_object import LogSourceObject, SourceObjectStatus
from app.persistence.models.customer import Customer
from app.persistence.models.customer_display_name import CustomerDisplayName
from app.persistence.models.logspace_presence import LogspacePresence
from app.persistence.models.saved_view import SavedView
from app.persistence.models.idempotency_key import IdempotencyKey
from app.persistence.models.notification import (
    CustomerNotificationChannel,
    NotificationRule,
    NotificationEvent,
    NotificationDelivery,
)

from app.persistence.models.analytics_pending_window import AnalyticsPendingWindow
from app.persistence.models.analytics_fact import (AnalyticsFact, AnalyticsFactLedger,
                                                   QuantityClassification)
from app.persistence.models.analytics_metric import AnalyticsMetric, MetricStatus
from app.persistence.models.analytics_rollup import (AnalyticsHourlyRollup, AnalyticsDailyRollup,
                                                     AnalyticsMonthlyRollup, DIMENSION_SLOTS)
from app.persistence.models.analytics_tenant_state import AnalyticsTenantState
from app.persistence.models.analytics_quality_issue import AnalyticsQualityIssue

__all__ = [
    "Base",
    "Job",
    "Chunk",
    "ChunkEntity",
    "EmbeddingQueueItem",
    "LogTransaction",
    "LogEntry",
    "LogEntryAssignment",
    "LogRegroupPending",
    "LogRegroupRun",
    "LogStreamFrontier",
    "LogSshSource",
    "LogSshFileCheckpoint",
    "LogSshFetchRun",
    "LogSourceObject",
    "SourceObjectStatus",
    "Customer",
    "CustomerDisplayName",
    "LogspacePresence",
    "SavedView",
    "IdempotencyKey",
    "CustomerNotificationChannel",
    "NotificationRule",
    "NotificationEvent",
    "NotificationDelivery",
    # --- analytics platform (Phase 1) ---
    "AnalyticsPendingWindow",
    "AnalyticsFact",
    "AnalyticsFactLedger",
    "QuantityClassification",
    "AnalyticsMetric",
    "MetricStatus",
    "AnalyticsHourlyRollup",
    "AnalyticsDailyRollup",
    "AnalyticsMonthlyRollup",
    "DIMENSION_SLOTS",
    "AnalyticsTenantState",
    "AnalyticsQualityIssue",
    # --- analytics registry (R1) ---
    "AnalyticsTransactionRegistry",
    "AnalyticsFieldRegistry",
    "AnalyticsRecordFact",
    # --- ML (M1) ---
    "AnalyticsFeatureSet",
    "AnalyticsPrediction",
    # --- stage 2 stream state (S4) ---
    "LogOpenStream",
    "LogPendingRequest",
]
