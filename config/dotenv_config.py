import os

from dotenv import load_dotenv
from dataclasses import dataclass

from logs.logger import get_logger
from errors.ConfigurationError import ConfigurationError


load_dotenv()
logger = get_logger()

def required_env(name: str, default=None):
    """
    Retrieve the value of a required environment variable.
    """
    value = os.getenv(name)

    if value is None and default is not None:
        return default

    if not value:
        logger.critical(
            "Required environment variable '%s' is missing",
            name
        )

        raise ConfigurationError(
            f"Missing environment variable: {name}"
        )

    return value


@dataclass(frozen=True, slots=True, kw_only=True)
class Config:
    """
    Configuration dataclass for storing environment variables and application settings.
    """
    
    DISCORD_WEBHOOK_URL       : str
    TELEGRAM_BOT_TOKEN        : str
    TELEGRAM_CHAT_ID          : str

    LOGTAIL_TOKEN             : str
    LOGTAIL_HOST              : str

    KAFKA_USERNAME            : str
    KAFKA_PASSWORD            : str
    KAFKA_CLIENT_ID           : str
    KAFKA_SERVER              : str
    KAFKA_SECURITY_PROTOCOL   : str
    KAFKA_SASL_MECHANISMS     : str
    KAFKA_SESSION_TIMEOUT_MS  : int
    KAFKA_TOPIC               : str

    BRONZE_LAYER_PATH         : str
    SILVER_LAYER_PATH         : str
    GOLD_LAYER_PATH           : str

    
    SNOWFLAKE_URL             : str
    SNOWFLAKE_USER            : str
    SNOWFLAKE_PASSWORD        : str
    SNOWFLAKE_DATABASE        : str
    SNOWFLAKE_SCHEMA          : str
    SNOWFLAKE_WAREHOUSE       : str

    GROQ_API_KEY              : str
    GROQ_API_KEY_1            : str
    GROQ_API_KEY_2            : str
    GROQ_API_KEY_3            : str
    GROQ_API_KEY_4            : str

    FAILOVER_GROQ_API_KEY     : str

    LLAMAFOUR_MAX_ATTEMPTS    : int
    LLAMATHREE_MAX_ATTEMPTS   : int
    LLAMATHREE_INSTANT_MAX_ATTEMPTS : int
    GROQ_MAX_ATTEMPTS         : int
    GPTOSS_MAX_ATTEMPTS       : int
    GPTOSS_SAFEGUARD_MAX_ATTEMPTS : int
    GPTOSS_20B_MAX_ATTEMPTS   : int
    FAILOVER_STATE_PATH       : str

    HOLIDAY_API_KEY           : str
    HOLIDAY_API_URL           : str
    HOLIDAY_COUNTRY           : str
    HOLIDAY_TIMEOUT           : int
    HOLIDAY_RETRIES           : int
    HOLIDAY_DELAY             : float

    
config = Config(
    DISCORD_WEBHOOK_URL=os.getenv("DISCORD_WEBHOOK_URL", ""),
    TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN", ""),
    TELEGRAM_CHAT_ID=os.getenv("TELEGRAM_CHAT_ID", ""),

    LOGTAIL_TOKEN=required_env("LOGTAIL_TOKEN"),
    LOGTAIL_HOST=required_env("LOGTAIL_HOST"),

    KAFKA_USERNAME=required_env("KAFKA_USERNAME"),
    KAFKA_PASSWORD=required_env("KAFKA_PASSWORD"),
    KAFKA_CLIENT_ID=required_env("KAFKA_CLIENT_ID"),
    KAFKA_SERVER=required_env("KAFKA_SERVER"),
    KAFKA_SECURITY_PROTOCOL=required_env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
    KAFKA_SASL_MECHANISMS=required_env("KAFKA_SASL_MECHANISMS", "PLAIN"),
    KAFKA_SESSION_TIMEOUT_MS=int(required_env("KAFKA_SESSION_TIMEOUT_MS", 45000)),
    KAFKA_TOPIC=required_env("KAFKA_TOPIC", "topic_0"),

    BRONZE_LAYER_PATH=required_env("BRONZE_LAYER_PATH", "output/bronze"),
    SILVER_LAYER_PATH=required_env("SILVER_LAYER_PATH"),
    GOLD_LAYER_PATH=required_env("GOLD_LAYER_PATH"),

    SNOWFLAKE_URL=required_env("SNOWFLAKE_URL"),
    SNOWFLAKE_USER=required_env("SNOWFLAKE_USER"),
    SNOWFLAKE_PASSWORD=required_env("SNOWFLAKE_PASSWORD"),
    SNOWFLAKE_DATABASE=required_env("SNOWFLAKE_DATABASE"),
    SNOWFLAKE_SCHEMA=required_env("SNOWFLAKE_SCHEMA"),
    SNOWFLAKE_WAREHOUSE=required_env("SNOWFLAKE_WAREHOUSE"),

    GROQ_API_KEY=os.getenv("GROQ_API_KEY", ""),
    GROQ_API_KEY_1=os.getenv("GROQ_API_KEY_1", ""),
    GROQ_API_KEY_2=os.getenv("GROQ_API_KEY_2", ""),
    GROQ_API_KEY_3=os.getenv("GROQ_API_KEY_3", ""),
    GROQ_API_KEY_4=os.getenv("GROQ_API_KEY_4", ""),
    FAILOVER_GROQ_API_KEY=os.getenv("FAILOVER_GROQ_API_KEY", ""),

    LLAMAFOUR_MAX_ATTEMPTS=int(required_env("LLAMAFOUR_MAX_ATTEMPTS", 15)),
    LLAMATHREE_MAX_ATTEMPTS=int(required_env("LLAMATHREE_MAX_ATTEMPTS", 15)),
    LLAMATHREE_INSTANT_MAX_ATTEMPTS=int(required_env("LLAMATHREE_INSTANT_MAX_ATTEMPTS", 15)),
    GROQ_MAX_ATTEMPTS=int(required_env("GROQ_MAX_ATTEMPTS", 15)),
    GPTOSS_MAX_ATTEMPTS=int(required_env("GPTOSS_MAX_ATTEMPTS", 15)),
    GPTOSS_SAFEGUARD_MAX_ATTEMPTS=int(required_env("GPTOSS_SAFEGUARD_MAX_ATTEMPTS", 15)),
    GPTOSS_20B_MAX_ATTEMPTS=int(required_env("GPTOSS_20B_MAX_ATTEMPTS", 15)),
    FAILOVER_STATE_PATH=required_env("FAILOVER_STATE_PATH", "Data/failover_state.json"),

    HOLIDAY_API_KEY=required_env("HOLIDAY_API_KEY"),
    HOLIDAY_API_URL=required_env("HOLIDAY_API_URL", "https://calendarific.com/api/v2/holidays"),
    HOLIDAY_COUNTRY=required_env("HOLIDAY_COUNTRY", "EG"),
    HOLIDAY_TIMEOUT=int(required_env("HOLIDAY_TIMEOUT", 10)),
    HOLIDAY_RETRIES=int(required_env("HOLIDAY_RETRIES", 3)),
    HOLIDAY_DELAY=float(required_env("HOLIDAY_DELAY", 0.25))
)
