import logging
import re
import requests

from config.dotenv_config import config


_MAX_MSG_LEN = 1500
_PLACEHOLDER_RE = re.compile(r"^https?://(localhost|127\.0\.0\.1|0\.0\.0\.0)")

logger = logging.getLogger("EarnestPipeline")


def _is_valid_webhook(url: str | None) -> bool:
    if not url or not url.strip():
        return False
    if _PLACEHOLDER_RE.match(url.strip()):
        logger.warning("Webhook URL looks like a placeholder: %s — skipping", url)
        return False
    return True


def _sanitize(msg: str) -> str:
    msg = re.sub(r"gsk_[A-Za-z0-9]{20,}", "gsk_***REDACTED***", msg)
    msg = re.sub(r"(Bearer\s+)[A-Za-z0-9._-]{20,}", r"\1***REDACTED***", msg, flags=re.IGNORECASE)
    if len(msg) > _MAX_MSG_LEN:
        msg = msg[:_MAX_MSG_LEN] + f"\n... [truncated — {len(msg)} total chars]"
    return msg



def send_discord_alert(step_name: str, error_message: str):
    url = getattr(config, "DISCORD_WEBHOOK_URL", "")
    if not _is_valid_webhook(url):
        return

    safe_message = _sanitize(error_message)
    payload = {
        "content": "🚨 **Spark Pipeline Error** 🚨",
        "embeds": [{
            "title": f"Failed at step: {step_name}",
            "description": f"```{safe_message}```",
            "color": 15158332,
        }],
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code not in (200, 204):
            logger.error("Failed to send Discord alert: %s", response.text)
    except Exception as e:
        logger.error("Network error while sending Discord alert: %s", e)



def send_telegram_alert(step_name: str, error_message: str):
    token = getattr(config, "TELEGRAM_BOT_TOKEN", "")
    chat_id = getattr(config, "TELEGRAM_CHAT_ID", "")
    if not token or not token.strip() or not chat_id or not chat_id.strip():
        return

    safe_message = _sanitize(error_message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": f"🚨 *Spark Pipeline Error*\n*Step:* {step_name}\n*Error:*\n```\n{safe_message}\n```",
        "parse_mode": "Markdown",
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error("Failed to send Telegram alert: %s", response.text)
    except Exception as e:
        logger.error("Network error while sending Telegram alert: %s", e)



def broadcast_alert(step_name: str, error_message: str) -> None:
    """
    Send pipeline failure alerts to all configured channels.

    Args:
        step_name (str): The name of the step that failed.
        error_message (str): The error message to send.
    """

    send_discord_alert(step_name, error_message)
    send_telegram_alert(step_name, error_message)
