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



def send_discord_alert(step_name: str, message: str, is_success: bool = False):
    url = getattr(config, "DISCORD_WEBHOOK_URL", "")
    if not _is_valid_webhook(url):
        return

    safe_message = _sanitize(message)
    
    title = "✅ **Spark Pipeline Success** ✅" if is_success else "🚨 **Spark Pipeline Error** 🚨"
    embed_title = f"Successful step: {step_name}" if is_success else f"Failed at step: {step_name}"
    color = 3066993 if is_success else 15158332 # Green vs Red

    payload = {
        "content": title,
        "embeds": [{
            "title": embed_title,
            "description": f"```{safe_message}```",
            "color": color,
        }],
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code not in (200, 204):
            logger.error("Failed to send Discord alert: %s", response.text)
    except Exception as e:
        logger.error("Network error while sending Discord alert: %s", e)



def send_telegram_alert(step_name: str, message: str, is_success: bool = False):
    token = getattr(config, "TELEGRAM_BOT_TOKEN", "")
    chat_id = getattr(config, "TELEGRAM_CHAT_ID", "")
    if not token or not token.strip() or not chat_id or not chat_id.strip():
        return

    safe_message = _sanitize(message)
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    
    title = "✅ *Spark Pipeline Success*" if is_success else "🚨 *Spark Pipeline Error*"
    step_label = "*Completed Step:*" if is_success else "*Failed Step:*"

    payload = {
        "chat_id": chat_id,
        "text": f"{title}\n{step_label} {step_name}\n*Details:*\n```\n{safe_message}\n```",
        "parse_mode": "Markdown",
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.error("Failed to send Telegram alert: %s", response.text)
    except Exception as e:
        logger.error("Network error while sending Telegram alert: %s", e)



def broadcast_alert(step_name: str, message: str, is_success: bool = False) -> None:
    """
    Send pipeline failure or success alerts to all configured channels.

    Args:
        step_name (str): The name of the step that triggered the alert.
        message (str): The error or success message to send.
        is_success (bool): Set to True for green success notifications, False for red failure errors.
    """

    send_discord_alert(step_name, message, is_success)
    send_telegram_alert(step_name, message, is_success)
