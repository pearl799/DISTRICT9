"""Structured logging for OpenClaw with file output."""

import logging
import sys
from pathlib import Path

_configured = False

# Log directory: ~/.openclaw-agent/logs/
LOG_DIR = Path.home() / ".openclaw-agent" / "logs"

_FMT = "[%(asctime)s] %(levelname)-7s %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str = "openclaw") -> logging.Logger:
    """Get a configured logger instance with console + file handlers."""
    global _configured
    logger = logging.getLogger(name)

    if not _configured:
        logger.setLevel(logging.DEBUG)

        # Console handler — INFO and above
        console = logging.StreamHandler(sys.stdout)
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(_FMT, datefmt="%H:%M:%S"))
        logger.addHandler(console)

        # File handlers — only if log dir can be created
        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)

            # launch.log — all INFO+ messages
            fh = logging.FileHandler(LOG_DIR / "launch.log", encoding="utf-8")
            fh.setLevel(logging.INFO)
            fh.setFormatter(logging.Formatter(_FMT, datefmt=_DATEFMT))
            logger.addHandler(fh)

            # error.log — WARNING+ only
            eh = logging.FileHandler(LOG_DIR / "error.log", encoding="utf-8")
            eh.setLevel(logging.WARNING)
            eh.setFormatter(logging.Formatter(_FMT, datefmt=_DATEFMT))
            logger.addHandler(eh)
        except OSError:
            pass  # no file logging if dir can't be created

        _configured = True

    return logger


log = get_logger()
