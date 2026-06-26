import os
import sys
import logging
import inspect

from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from typing import Final

try:
    from logtail import LogtailHandler
except Exception:
    LogtailHandler = None


_DEFAULT_LOG_NAME: Final[str] = "EarnestPipeline"
_DEFAULT_LOG_PATH: Final[str] = "logs"


def _build_logtail_handler(token: str, host: str, formatter: logging.Formatter):
    if not token or LogtailHandler is None:
        return None

    sdk_params = inspect.signature(LogtailHandler.__init__).parameters
    kwargs = {"source_token": token}

    if host:
        if "host" in sdk_params:
            kwargs["host"] = host
        elif "endpoint" in sdk_params:
            kwargs["endpoint"] = host if host.startswith("http") else f"https://{host}"

    handler = LogtailHandler(**kwargs)
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    return handler


def get_logger(name: str | None = None, path: str | None = None) -> logging.Logger:
    """
    Returns a configured logger instance.

    Args:
        name (str, optional): The name of the logger. Defaults to "EarnestPipeline".
        path (str, optional): The path to the log directory. Defaults to "logs".

    Returns:
        logging.Logger: The configured logger instance.
    """
   
    load_dotenv()

    log_name = name or os.getenv("LOG_NAME", _DEFAULT_LOG_NAME)
    log_path = path or os.getenv("LOG_PATH", _DEFAULT_LOG_PATH)
    logtail_token = os.getenv("LOGTAIL_TOKEN", "").strip()
    logtail_host = os.getenv("LOGTAIL_HOST", "").strip()

    os.makedirs(log_path, exist_ok=True)

    logger = logging.getLogger(log_name)
    if getattr(logger, "_earnest_configured", False):
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    try:
        file_handler = RotatingFileHandler(
            filename=os.path.join(log_path, "spark_app.log"),
            maxBytes=5_000_000,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    except (OSError, PermissionError) as e:
        logger.warning("Could not initialize file logger: %s", e)

    try:
        logtail_handler = _build_logtail_handler(logtail_token, logtail_host, formatter)
        if logtail_handler is not None:
            logger.addHandler(logtail_handler)
            logger.info("BetterStack Logtail handler attached")
        else:
            logger.warning("BetterStack Logtail handler not attached: missing token or package")
    except Exception as exc:
        logger.warning("BetterStack Logtail handler failed to attach: %s", exc, exc_info=True)

    logger._earnest_configured = True
    return logger
