import contextlib
import json
import logging
import sys
import traceback
from collections.abc import Callable, Iterator, Mapping
from logging import Logger, LogRecord
from typing import Any, Protocol

# Global logger instance
LOGGER: Logger | None = None

_MESSAGE_LOGGING_METHODS = {
    'debug',
    'info',
    'warning',
    'warn',
    'error',
    'critical',
    'exception',
    'fatal',
}


class LogMethod(Protocol):
    def __call__(self, msg: Any, *args: Any, **kwargs: Any) -> Any: ...


def __getattr__(name: str) -> LogMethod | Any:
    """
    Forwards log method calls (debug, info, error etc.) to the global LOGGER instance.
    This allows usage like:
        from iceberg_loader import logger
        logger.info("message")
    """

    def wrapper(msg: Any, *args: Any, **kwargs: Any) -> Any:
        if LOGGER is None:
            configure_logging()

        target: Callable[..., Any] = getattr(LOGGER, name)

        if name in _MESSAGE_LOGGING_METHODS:
            # skip stack frames when displaying log so the original logging frame is displayed
            # We want to skip: wrapper -> __getattr__ logic
            # logging module usually handles stacklevel=1 as "caller of logging method"
            kwargs.setdefault('stacklevel', 2)
            if name == 'exception':
                kwargs.setdefault('stacklevel', 3)

        return target(msg, *args, **kwargs)

    if name in _MESSAGE_LOGGING_METHODS:
        wrapper.__name__ = name
        return wrapper

    # Allow accessing other attributes of LOGGER if initialized
    if LOGGER is None:
        configure_logging()

    return getattr(LOGGER, name)

    raise AttributeError(f'module {__name__} has no attribute {name}')


def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None:
    """Forwards metrics call to LOGGER as an info log with extra data."""
    if LOGGER:
        LOGGER.info(f'METRIC: {name}', extra={'metrics': extra}, stacklevel=stacklevel)


@contextlib.contextmanager
def suppress_and_warn(msg: str) -> Iterator[None]:
    """Context manager to catch exceptions, log them as warnings, and suppress them."""
    try:
        yield
    except Exception:
        # We access the module-level 'warning' which goes through __getattr__
        # But here we are inside the module, so we need to be careful.
        # It's safer to use the global LOGGER directly if available.
        if LOGGER:
            LOGGER.warning(msg, exc_info=True)


def is_logging() -> bool:
    return LOGGER is not None


def log_level() -> str:
    if not LOGGER:
        return 'NOTSET'
    return logging.getLevelName(LOGGER.level)


def pretty_format_exception() -> str:
    return traceback.format_exc()


class TextFormatter(logging.Formatter):
    """
    Formatter for human-readable text logs.
    Includes metrics dump if present in record.
    """

    def format(self, record: LogRecord) -> str:
        s = super().format(record)
        # dump metrics dictionary nicely if present
        metrics_data = getattr(record, 'metrics', None)
        if metrics_data:
            s = f'{s} | metrics={json.dumps(metrics_data)}'
        return s


class JsonFormatter(logging.Formatter):
    """
    Formatter for JSON logs, suitable for production/cloud environments.
    """

    def __init__(self, component: str = 'iceberg-loader', version: Mapping[str, str] | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.component = component
        self.version = version

    def format(self, record: LogRecord) -> str:
        log_obj = {
            'time': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
            'component': self.component,
            'module': record.module,
            'line': record.lineno,
            'process': record.process,
            'thread': record.threadName,
        }

        if record.exc_info:
            log_obj['exception'] = self.formatException(record.exc_info)

        # Include extra attributes that might have been passed
        if hasattr(record, 'metrics'):
            log_obj['metrics'] = record.metrics

        if self.version:
            log_obj['version'] = self.version

        return json.dumps(log_obj)


def configure_logging(
    level: str = 'INFO',
    log_format: str = 'TEXT',  # "TEXT" or "JSON"
    component: str = 'iceberg-loader',
    version: Mapping[str, str] | None = None,
) -> Logger:
    """
    Initializes the global logger.
    """
    global LOGGER

    # Create or get root-like logger for library
    # We use a specific name to avoid hijacking root logger unless intended
    logger_name = 'iceberg_loader'

    # If we want to capture everything, we might configure the root logger,
    # but usually a library should configure its own.
    # However, for a standalone tool/app, configuring root is common.
    # Let's configure 'iceberg_loader' specifically.
    logger = logging.getLogger(logger_name)
    logger.setLevel(level.upper())

    # Remove existing handlers to avoid duplicates on re-configuration
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create handler (stderr)
    handler = logging.StreamHandler(sys.stderr)

    # Set formatter
    formatter: logging.Formatter
    if log_format.upper() == 'JSON':
        formatter = JsonFormatter(component=component, version=version)
    else:
        # Simple text format
        fmt_str = '{asctime} [{levelname}] {name}: {message}'
        formatter = TextFormatter(fmt=fmt_str, style='{')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Prevent propagation if we are handling it ourselves to avoid double logging
    # if the parent app also has logging configured.
    logger.propagate = False

    LOGGER = logger
    return logger


def get_logger() -> Logger:
    """Returns the configured logger, or a default one if not configured."""
    if LOGGER:
        return LOGGER
    # Fallback to a basic config if accessed before explicit init
    return configure_logging()
