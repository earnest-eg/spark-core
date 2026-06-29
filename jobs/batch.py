from datetime import datetime
from zoneinfo import ZoneInfo

from .BasePipeline import BasePipeline

from pyspark.sql import SparkSession

from logs.logger import get_logger


logger = get_logger()


class BatchPipeline(BasePipeline):
    """
    Orchestrates the batch ETL pipeline for all sellers.
    Load -> Bronze -> Transform -> Silver
    """

    def __init__(self, spark: SparkSession, skip_llm: bool = False):
        """
        Initialize BatchPipeline.
        Call the parent class constructor to set up the SparkSession.
        Set the timestamp to be used for all transformations in this run.

        Args:
            spark (SparkSession): The SparkSession to use.
            skip_llm (bool): If True, bypass LLM imputation entirely.
        """
        super().__init__(spark)
        self.timestamp = datetime.now(tz=ZoneInfo("Africa/Cairo")).isoformat()
        self.skip_llm = skip_llm

    def run(self, seller_files: dict) -> int:
        """
        Run the pipeline for all sellers.

        Args:
            seller_files (dict): A dictionary mapping seller names to their CSV file paths.

        Returns:
            int: The number of sellers successfully processed.
        """
        for seller, file_path in seller_files.items():
            try:
                logger.info(f"Reading CSV for {seller} from {file_path}")

                raw_df = self.spark.read.option("header", "true").csv(file_path)
                clean_df = self._transform(seller, raw_df, self.skip_llm)

                table_name = "STG_ALL_SELLERS_PRODUCTS"

                self.write_to_snowflake(clean_df, table_name, mode="append")

            except Exception as e:
                logger.error(f"Failed to process batch for {seller}: {e}", exc_info=True)
