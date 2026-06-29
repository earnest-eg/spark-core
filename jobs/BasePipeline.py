from abc import ABC, abstractmethod

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

from config.dotenv_config import config
from config.pipeline_config import SELLER_CONFIGS

from logs.logger import get_logger
from logs.alert import broadcast_alert
from errors.PipelineBaseError import PipelineBaseError

from transformers.date_time_extractor import add_date, add_time
from transformers.cleaner import (
    drop_and_rename, fill_nulls, drop_temps,
    remove_duplicate, cast_data_types,
    drop_unneeded_columns
)
from transformers.feature_extractor import (
    normalize_prices, calculate_discount, normalize_availability,
    normalize_categories, extract_units_weight_count,
    extract_brand, tag_talabat_seller,
)
from transformers.llm_imputer import fill_na_with_llm

logger = get_logger()


def _safe_broadcast_alert(step_name: str, error_message: str) -> None:
    try:
        broadcast_alert(step_name, error_message)
    except Exception as alert_error:
        logger.error(f"Failed to broadcast alert for step={step_name}: {alert_error}")


class BasePipeline(ABC):
    """
    Base class for all pipelines.

    Provides common functionality for all pipelines, such as:
    - Initial cleanup and normalization
    - LLM imputation
    - Feature engineering
    - Writing to Snowflake
    """

    def __init__(self, spark: SparkSession):
        """
        Initializes the base pipeline with a SparkSession.
        
        Args:
            spark (SparkSession): The SparkSession to use for the pipeline.
        """
        self.spark = spark

    @abstractmethod
    def run(self, *args, **kwargs):
        """
        Runs the pipeline and returns the transformed DataFrame.
        """
        pass

    def _transform(self, seller: str, df: SparkDataFrame, skip_llm: bool = False) -> SparkDataFrame:
        cfg = SELLER_CONFIGS.get(seller, {})
        cfg["seller_name"] = seller
        if skip_llm:
            cfg["skip_llm"] = True

        try:
            df = (
                df
                .transform(drop_and_rename(cfg))
                .transform(normalize_prices(cfg))
                .transform(calculate_discount(cfg))
                .transform(normalize_availability(cfg))
                .transform(normalize_categories(cfg))
            )
            df = df.localCheckpoint(eager=True)

            df = (
                df
                .transform(extract_units_weight_count(cfg))
                .transform(extract_brand(cfg))
                .transform(fill_na_with_llm(cfg))
                .transform(fill_nulls(cfg))
            )
            df = df.localCheckpoint(eager=True)


            df = (
                df
                .transform(add_date(cfg))
                .transform(add_time(cfg))
                .transform(drop_temps)
                .transform(remove_duplicate(cfg))
                .transform(cast_data_types(cfg))
                .transform(tag_talabat_seller(cfg))
                .transform(drop_unneeded_columns)
            )
            return df
        except PipelineBaseError as e:
            logger.error(f"Pipeline failed for {seller} at [{e.step_name}]: {e.message}")
            _safe_broadcast_alert(e.step_name, e.message)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during transformation for {seller}: {e}", exc_info=True)
            raise

    def write_to_snowflake(self, df: SparkDataFrame, table_name: str, mode: str = "append"):
        logger.info(f"Writing data to Snowflake table: {table_name}")
        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas
            from pyspark.sql.functions import col, year, when, current_timestamp, current_date
            from pyspark.sql.types import DateType, TimestampType

            for f in df.schema.fields:
                if isinstance(f.dataType, DateType):
                    df = df.withColumn(
                        f.name,
                        when((year(col(f.name)) > 9999) | (year(col(f.name)) < 1), current_date())
                        .otherwise(col(f.name))
                    )
                elif isinstance(f.dataType, TimestampType):
                    df = df.withColumn(
                        f.name,
                        when((year(col(f.name)) > 9999) | (year(col(f.name)) < 1), current_timestamp())
                        .otherwise(col(f.name))
                    )

            pdf = df.toPandas()
            pdf.columns = [c.upper() for c in pdf.columns]

            conn = snowflake.connector.connect(
                account=config.SNOWFLAKE_URL.replace(".snowflakecomputing.com", ""),
                user=config.SNOWFLAKE_USER,
                password=config.SNOWFLAKE_PASSWORD,
                database=config.SNOWFLAKE_DATABASE,
                schema=config.SNOWFLAKE_SCHEMA,
                warehouse=config.SNOWFLAKE_WAREHOUSE,
            )

            try:
                if mode == "overwrite":
                    conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

                success, nchunks, nrows, _ = write_pandas(
                    conn, pdf, table_name, auto_create_table=True
                )
                logger.info(
                    f"Successfully written to Snowflake: {table_name} "
                    f"({nrows} rows in {nchunks} chunks)"
                )
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to write to Snowflake: {e}", exc_info=True)
            raise