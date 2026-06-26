import time
import requests
from typing import Callable

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F

from config.dotenv_config import config
from utils.decorators import safe_step
from logs.logger import get_logger
import datetime


logger = get_logger(__name__)


HOLIDAY_API_KEY   = config.HOLIDAY_API_KEY
HOLIDAY_API_URL   = config.HOLIDAY_API_URL
HOLIDAY_COUNTRY   = config.HOLIDAY_COUNTRY

HOLIDAY_TIMEOUT   = config.HOLIDAY_TIMEOUT
HOLIDAY_RETRIES   = config.HOLIDAY_RETRIES
HOLIDAY_DELAY     = config.HOLIDAY_DELAY



def _fetch_year_holidays(year: int, session: requests.Session) -> dict[str, dict]:
    """fetches holidays for a given year from the holiday API."""
    url    = HOLIDAY_API_URL
    params = {"api_key": HOLIDAY_API_KEY, "country": HOLIDAY_COUNTRY, "year": year}

    for attempt in range(1, HOLIDAY_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=HOLIDAY_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()

            result = {}
            for h in data.get("response", {}).get("holidays", []):
                date_iso   = h["date"]["iso"][:10]
                htype_list = h.get("type", [])
                htype      = htype_list[0] if htype_list else "observance"

                if date_iso not in result:
                    result[date_iso] = {"name": h["name"], "type": htype}

            return result

        except requests.RequestException as exc:
            if attempt == HOLIDAY_RETRIES:
                raise
            wait = attempt * 0.5
            logger.warning("[holiday fetch] Year %d, attempt %d failed (%s). Retrying in %.1fs…", year, attempt, exc, wait)
            time.sleep(wait)

    return {}


def _build_spark_holiday_maps(holiday_dict):
    """
    Builds a Spark map expression for holiday lookups based on the provided holiday dictionary.
    """
    if holiday_dict:
        mapping_exprs = []
        for k, v in holiday_dict.items():
            mapping_exprs.extend([F.lit(k), F.lit(v["name"])])
        return F.create_map(mapping_exprs)
    return F.create_map([F.lit(""), F.lit("")]) 


_HOLIDAY_YEAR_CACHE: dict[int, dict] = {}

@safe_step("Add Date Features")
def add_date(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Adds date features to the DataFrame.
    
    Args:
        config (dict): Configuration dictionary containing timestamp_col and timezone.
        
    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A function that adds date features to the DataFrame.

    Process Description:
        1. Fetches holidays for the current year from the holiday API.
        2. Builds a Spark map expression for holiday lookups.
        3. Adds date features to the DataFrame.

    Requires:
        - Holiday API key
        - Holiday API URL
        - Holiday country
        - SparkSession
        - PySpark DataFrame
        - Timestamp column (default: scraping_time)
        - Timezone (default: Africa/Cairo)

    Output Schema:
        - date_id: bigint
        - date: date
        - day: integer
        - year: integer
        - month: integer
        - quarter: integer
        - month_name: string
        - day_name: string
        - day_of_week: integer
        - day_of_year: integer
        - week_of_year: integer
        - is_weekend: boolean
        - is_holyday: boolean
        - holyday_name: string
        - is_workday: boolean
        - is_leap_year: boolean
        - is_month_start: boolean
        - is_month_end: boolean
        - is_quarter_start: boolean
        - is_quarter_end: boolean
        - is_year_start: boolean
        - is_year_end: boolean
        - is_week_start: boolean
        - is_week_end: boolean
        - season: string
        - is_winter: boolean
        - is_spring: boolean
        - is_summer: boolean
        - is_autumn: boolean
    """
    config      = config or {}
    time_col    = config.get("timestamp_col", "scraping_time")
    target_tz   = config.get("timezone", "Africa/Cairo")

    current_now = datetime.datetime.now()
    year = current_now.year
    
    if year not in _HOLIDAY_YEAR_CACHE:
        with requests.Session() as _session:
            _HOLIDAY_YEAR_CACHE[year] = _fetch_year_holidays(year, _session)
    egypt_api_holidays = _HOLIDAY_YEAR_CACHE[year]
    holiday_map = _build_spark_holiday_maps(egypt_api_holidays)

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        if time_col not in df.columns:
            return df

        input_ts = F.col(time_col).cast("timestamp")
        now = F.current_timestamp()
        raw_ts = F.when(input_ts > now, now).otherwise(F.coalesce(input_ts, now))
        
        ts = F.from_utc_timestamp(raw_ts, target_tz)
        date_col = F.to_date(ts)

        date_str_col      = F.date_format(date_col, "yyyy-MM-dd")
        holiday_name_expr = holiday_map[date_str_col]

        return (
            df
            .withColumns({
                'date_id'          : F.date_format(ts, "yyyyMMdd").cast("bigint"),
                'date'             : date_col,
                'day'              : F.dayofmonth(ts),
                'year'             : F.year(ts),
                'month'            : F.month(ts),
                'quarter'          : F.quarter(ts),
                'month_name'       : F.date_format(ts, "MMMM"),
                'day_name'         : F.date_format(ts, "EEEE"),
                'day_of_week'      : F.date_format(ts, "u").cast("int"),
                'day_of_year'      : F.dayofyear(ts),
                'week_of_year'     : F.weekofyear(ts),
                'is_weekend'       : F.when(F.date_format(ts, "u").isin("5", "6"), F.lit(True)).otherwise(F.lit(False)),
                'is_holyday'       : holiday_name_expr.isNotNull(),
                'holyday_name'     : F.coalesce(holiday_name_expr, F.lit("N/A")),
                'is_workday'       : F.when(F.date_format(ts, "u").isin("7", "1", "2", "3", "4"), F.lit(True)).otherwise(F.lit(False)),
                'is_leap_year'     : F.when((F.year(ts) % 4 == 0) & ((F.year(ts) % 100 != 0) | (F.year(ts) % 400 == 0)), F.lit(True)).otherwise(F.lit(False)),
                'is_month_start'   : F.when(F.dayofmonth(ts) == 1, F.lit(True)).otherwise(F.lit(False)),
                'is_month_end'     : F.when(F.last_day(ts) == date_col, F.lit(True)).otherwise(F.lit(False)),
                'is_quarter_start' : F.when(F.month(ts).isin(1, 4, 7, 10) & (F.dayofmonth(ts) == 1), F.lit(True)).otherwise(F.lit(False)),
                'is_quarter_end'   : F.when(F.month(ts).isin(3, 6, 9, 12) & (date_col == F.last_day(ts)), F.lit(True)).otherwise(F.lit(False)),
                'is_year_start'    : F.when((F.month(ts) == 1) & (F.dayofmonth(ts) == 1), F.lit(True)).otherwise(F.lit(False)),
                'is_year_end'      : F.when((F.month(ts) == 12) & (F.dayofmonth(ts) == 31), F.lit(True)).otherwise(F.lit(False)),
                'is_week_start'    : F.when(F.date_format(ts, "u") == "1", F.lit(True)).otherwise(F.lit(False)),
                'is_week_end'      : F.when(F.date_format(ts, "u") == "7", F.lit(True)).otherwise(F.lit(False)),
                'season'           : F.when(F.month(ts).isin(12, 1, 2), F.lit("Winter")).when(F.month(ts).isin(3, 4, 5), F.lit("Spring")).when(F.month(ts).isin(6, 7, 8), F.lit("Summer")).when(F.month(ts).isin(9, 10, 11), F.lit("Autumn")).otherwise(F.lit("N/A")),
                'is_winter'        : F.when(F.month(ts).isin(12, 1, 2), F.lit(True)).otherwise(F.lit(False)),
                'is_spring'        : F.when(F.month(ts).isin(3, 4, 5), F.lit(True)).otherwise(F.lit(False)),
                'is_summer'        : F.when(F.month(ts).isin(6, 7, 8), F.lit(True)).otherwise(F.lit(False)),
                'is_autumn'        : F.when(F.month(ts).isin(9, 10, 11), F.lit(True)).otherwise(F.lit(False)),
            })
        )

    return transform


@safe_step("Add Time Features")
def add_time(config: dict = None) -> Callable[[SparkDataFrame], SparkDataFrame]:
    """
    Adds time features to the DataFrame.

    Args:
        config (dict): Configuration dictionary containing timestamp_col and timezone.

    Returns:
        Callable[[SparkDataFrame], SparkDataFrame]: A function that adds time features to the DataFrame.

    Process Description:
        1. Fetches time features from the current time.
        2. Adds time features to the DataFrame.

    Requires:
        - SparkSession
        - PySpark DataFrame
        - Timestamp column (default: scraping_time)
        - Timezone (default: Africa/Cairo)

    Output Schema:
        - time_id: int
        - hour: integer
        - minute: integer
        - second: integer
        - am_pm: string
        - is_morning: boolean
        - is_afternoon: boolean
        - is_evening: boolean
        - is_night: boolean
        - hour_range: string
        - is_rush_hour: boolean
        - is_late_night: boolean
    """
    config = config or {}
    time_col  = config.get("timestamp_col", "scraping_time")
    target_tz = config.get("timezone", "Africa/Cairo")

    def transform(df: SparkDataFrame) -> SparkDataFrame:
        if time_col not in df.columns:
            return df

        input_ts = F.col(time_col).cast("timestamp")
        now = F.current_timestamp()
        raw_ts = F.when(input_ts > now, now).otherwise(F.coalesce(input_ts, now))
        ts = F.from_utc_timestamp(raw_ts, target_tz)

        return (
            df
            .withColumns({
                'time_id'        : F.date_format(ts, "HHmmss").cast("int"),
                'time_12'        : F.date_format(ts, "hh:mm:ss"),
                'time_24'        : F.date_format(ts, "HH:mm:ss"),
                'hour_12'        : F.date_format(ts, "hh").cast("int"),
                'hour_24'        : F.date_format(ts, "HH").cast("int"),
                'minute'         : F.date_format(ts, "mm").cast("int"),
                'second'         : F.date_format(ts, "ss").cast("int"),
                'meridiem'       : F.date_format(ts, "a"),
                'is_am'          : F.when(F.date_format(ts, "a") == "AM", F.lit(True)).otherwise(F.lit(False)),
                'is_pm'          : F.when(F.date_format(ts, "a") == "PM", F.lit(True)).otherwise(F.lit(False)),
                'part_of_day'    : F.when(F.hour(ts).between(5, 11), "morning").when(F.hour(ts).between(12, 16), "afternoon").when(F.hour(ts).between(17, 20), "evening").otherwise("night"),
                'is_morning'     : F.when(F.hour(ts).between(5, 11), F.lit(True)).otherwise(F.lit(False)),
                'is_afternoon'   : F.when(F.hour(ts).between(12, 16), F.lit(True)).otherwise(F.lit(False)),
                'is_evening'     : F.when(F.hour(ts).between(17, 20), F.lit(True)).otherwise(F.lit(False)),
                'is_night'       : F.when((F.hour(ts) >= 21) | (F.hour(ts) <= 4), F.lit(True)).otherwise(F.lit(False)),
                'is_noon'        : F.when((F.date_format(ts, "HH") == "12") & (F.date_format(ts, "mm") == "00") & (F.date_format(ts, "ss") == "00"), F.lit(True)).otherwise(F.lit(False)),
                'is_midnight'    : F.when((F.date_format(ts, "HH") == "00") & (F.date_format(ts, "mm") == "00") & (F.date_format(ts, "ss") == "00"), F.lit(True)).otherwise(F.lit(False)),
                'is_midday'      : F.when((F.date_format(ts, "HH") == "12") & (F.date_format(ts, "mm") == "00") & (F.date_format(ts, "ss") == "00"), F.lit(True)).otherwise(F.lit(False)),
            }).drop(time_col)
        )

    return transform