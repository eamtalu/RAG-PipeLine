# base.py — Abstract Log Parser Interface
#
#   Mirrors data_ingestion's BaseParser/Strategy shape. Every log-format parser implements
#   parse(text) -> list[LogRecord]. The rest of the pipeline doesn't care which format it is.

"""Abstract log-parser interface — every log format implements this."""

from abc import ABC, abstractmethod

from app.services.mnp_log_ingestion.parsers.data_class.log_record import LogRecord


class BaseLogParser(ABC):
    @abstractmethod
    def parse(self, text: str) -> list[LogRecord]:
        """Parse a full log file's text into normalised LogRecords (one per timestamped entry)."""
