"""Optional rotating file-log handler (app.main.setup_logging).

The app logs to stdout/stderr by default; when settings.log_file is set it ALSO writes to a rotating
file — useful under bare `uvicorn main:app` where stdout isn't captured durably.

Covered:
- no file handler when log_file is empty (default);
- a RotatingFileHandler is added and actually writes when log_file is set;
- `~` in the path is expanded;
- setup_logging is idempotent (no duplicate handlers on repeated calls);
- rotation params are honored.
"""

import logging
import os
from logging.handlers import RotatingFileHandler

import pytest

from app import main as appmain
from app.settings import settings


@pytest.fixture(autouse=True)
def _clean_root_handlers():
    """Snapshot/restore root handlers so these tests don't leak handlers into the rest of the suite."""
    root = logging.getLogger()
    saved = list(root.handlers)
    saved_level = root.level
    try:
        yield
    finally:
        for h in list(root.handlers):
            if h not in saved:
                h.close()
                root.removeHandler(h)
        root.handlers[:] = saved
        root.setLevel(saved_level)


def _file_handlers():
    return [h for h in logging.getLogger().handlers if isinstance(h, RotatingFileHandler)]


def test_no_file_handler_when_log_file_empty(monkeypatch):
    monkeypatch.setattr(settings, "log_file", "")
    logging.getLogger().handlers[:] = []  # start clean
    appmain.setup_logging()
    assert _file_handlers() == []
    # but a stream handler is present
    assert any(isinstance(h, logging.StreamHandler) for h in logging.getLogger().handlers)


def test_file_handler_added_and_writes(monkeypatch, tmp_path):
    path = tmp_path / "rag-backend.log"
    monkeypatch.setattr(settings, "log_file", str(path))
    logging.getLogger().handlers[:] = []
    appmain.setup_logging()

    fhs = _file_handlers()
    assert len(fhs) == 1
    logging.getLogger("test.ssh").info("SSH poll marker line")
    fhs[0].flush()
    assert path.exists()
    assert "SSH poll marker line" in path.read_text()


def test_tilde_path_is_expanded(monkeypatch, tmp_path):
    # point HOME at tmp so "~/x.log" resolves under the sandbox
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(settings, "log_file", "~/rag.log")
    logging.getLogger().handlers[:] = []
    appmain.setup_logging()
    fhs = _file_handlers()
    assert len(fhs) == 1
    assert fhs[0].baseFilename == str(tmp_path / "rag.log")


def test_setup_logging_is_idempotent(monkeypatch, tmp_path):
    path = tmp_path / "rag.log"
    monkeypatch.setattr(settings, "log_file", str(path))
    logging.getLogger().handlers[:] = []
    appmain.setup_logging()
    appmain.setup_logging()
    appmain.setup_logging()
    assert len(_file_handlers()) == 1  # no duplicates
    assert sum(1 for h in logging.getLogger().handlers
               if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler)) == 1


def test_rotation_params_are_honored(monkeypatch, tmp_path):
    path = tmp_path / "rag.log"
    monkeypatch.setattr(settings, "log_file", str(path))
    monkeypatch.setattr(settings, "log_file_max_bytes", 1234)
    monkeypatch.setattr(settings, "log_file_backup_count", 7)
    logging.getLogger().handlers[:] = []
    appmain.setup_logging()
    fh = _file_handlers()[0]
    assert fh.maxBytes == 1234
    assert fh.backupCount == 7
