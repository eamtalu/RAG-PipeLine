from app.persistence.models.consumer_cursor import ConsumerCursor  # noqa: F401
from app.persistence.models.analytics_transaction_registry import AnalyticsTransactionRegistry  # noqa: F401
from app.persistence.models.analytics_field_registry import AnalyticsFieldRegistry  # noqa: F401
from app.persistence.models.analytics_field_meaning import (  # noqa: F401
    KINDS as FIELD_MEANING_KINDS, AnalyticsFieldMeaning)
from app.persistence.models.analytics_settlement import (AnalyticsSettledRow,  # noqa: F401
                                                        AnalyticsSettlement)
from app.persistence.models.analytics_lookup import (AnalyticsLookup,  # noqa: F401
                                                    AnalyticsLookupValue)
from app.persistence.models.analytics_record_fact import AnalyticsRecordFact  # noqa: F401
from app.persistence.models.analytics_ml import (AnalyticsFeatureSet,  # noqa: F401
                                                AnalyticsPrediction)
from app.persistence.models.analytics_forecast import (AnalyticsForecastAccuracy,  # noqa: F401
                                                       AnalyticsForecastRun, AnalyticsForecastSeries)
from app.persistence.models.analytics_at_risk import (AnalyticsAtRiskCheck,  # noqa: F401
                                                      AnalyticsAtRiskDelivery, AnalyticsAtRiskRouteProfile,
                                                      AnalyticsAtRiskSettings, AnalyticsAtRiskTenantState)
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
from app.persistence.models.log_stitch_checkpoint import LogStitchCheckpoint
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
from app.persistence.models.teams_binding import TeamsTenantBinding
from app.persistence.models.tenant_site import TenantSite  # noqa: F401
from app.persistence.models.teams_conversation_turn import TeamsConversationTurn
from app.persistence.models.teams_conversation import TeamsConversation
from app.persistence.models.access import (  # noqa: F401
    AccessApproval,
    AccessCapability,
    AccessRole,
    AccessRoleCapability,
    AuditEvent,
    TeamsUser,
    TeamsUserCapability,
    TeamsUserSite,
)
from app.persistence.models.advisory import Advisory, AdvisoryAgent, AdvisoryAgentRun  # noqa: F401
from app.persistence.models.watch import Watch, WatchEvent, WatchNotNow  # noqa: F401
from app.persistence.models.task import TeamsTask, TeamsTaskRun  # noqa: F401
from app.persistence.models.notification_group import (  # noqa: F401
    AdvisoryAnnouncement,
    NotificationGroup,
    NotificationGroupChannel,
    NotificationGroupMember,
)
from app.persistence.models.agent_run import AgentRun
from app.persistence.models.agent_trace import AgentTrace
from app.persistence.models.agent_in_flight import AgentInFlight
from app.persistence.models.quota_window import QuotaWindow
from app.persistence.models.usage_daily import UsageDaily
from app.persistence.models.tenant_secret import TenantSecret

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
    "LogStitchCheckpoint",
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
    "AnalyticsForecastRun",
    "AnalyticsForecastSeries",
    "AnalyticsForecastAccuracy",
    # --- deliveries at risk (chunks 143-150) ---
    "AnalyticsAtRiskDelivery",
    "AnalyticsAtRiskCheck",
    "AnalyticsAtRiskRouteProfile",
    "AnalyticsAtRiskSettings",
    "AnalyticsAtRiskTenantState",
    # --- stage 2 stream state (S4) ---
    "LogOpenStream",
    "LogPendingRequest",
    # --- Teams bot (tenant binding + conversation memory) ---
    "TeamsTenantBinding",
    "TeamsConversationTurn",
    "TeamsConversation",
    # --- access control (stage D, step 7) ---
    "AccessRole",
    "AccessCapability",
    "AccessRoleCapability",
    "TeamsUser",
    "TeamsUserSite",
    "TeamsUserCapability",
    "AccessApproval",
    "AuditEvent",
    # --- advisories (stage D, step 9) ---
    "Advisory",
    "AdvisoryAgent",
    "AdvisoryAgentRun",
    # --- tasks (stage D, step 10) ---
    "TeamsTask",
    "TeamsTaskRun",
    # --- notification groups (stage D, step 12) ---
    "AdvisoryAnnouncement",
    "NotificationGroup",
    "Watch",
    "WatchEvent",
    "WatchNotNow",
    "NotificationGroupChannel",
    "NotificationGroupMember",
    "AgentRun",
    "AgentTrace",
    "AgentInFlight",
    "QuotaWindow",
    "UsageDaily",
    "TenantSecret",
]
