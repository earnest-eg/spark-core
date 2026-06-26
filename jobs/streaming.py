from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame as SparkDataFrame
from .BasePipeline import BasePipeline

from config.dotenv_config import config
from config.pipeline_config import SELLER_CONFIGS

from logs.logger import get_logger


logger = get_logger()


class StreamingPipeline(BasePipeline):
    """
    Orchestrates the Kafka → Spark Structured Streaming pipeline.
    Kafka -> Transform -> Silver
    """

    def __init__(self, spark: SparkSession, schema: StructType):
        super().__init__(spark)
        self.schema = schema 

    def run(self) -> None:
        """
        Run the streaming pipeline.
        Reads data from Kafka, parses it, and writes it to the Bronze and Silver layers.
        """
        logger.info("Starting Kafka Streaming Pipeline...")

        raw_df = self._read_kafka()
        parsed_df = self._parse(raw_df)

        bronze_query = self._write_bronze(parsed_df)
        silver_query = self._write_silver(parsed_df)

        self.spark.streams.awaitAnyTermination()


    def _read_kafka(self) -> SparkDataFrame:
        """
        Reads raw data from Kafka topic using credentials from the environment config.

        Example JAAS config structure (values come from env vars — do NOT hardcode):
            jaas_config = 'org.apache.kafka.common.security.plain.PlainLoginModule
                           required username="<KAFKA_USERNAME>" password="<KAFKA_PASSWORD>";'
        """
        jaas_config = f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{config.KAFKA_USERNAME}" password="{config.KAFKA_PASSWORD}";'
        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", config.KAFKA_SERVER)
            .option("subscribe", config.KAFKA_TOPIC)
            .option("kafka.security.protocol", config.KAFKA_SECURITY_PROTOCOL)
            .option("kafka.sasl.mechanism", config.KAFKA_SASL_MECHANISMS)
            .option("kafka.sasl.jaas.config", jaas_config)
            .option("startingOffsets", "latest")
            .load()
        )


    def _parse(self, raw_df) -> SparkDataFrame:
        """
        Parses the raw data from Kafka.

        Args:
            raw_df (SparkSparkDataFrame): The raw data from Kafka.

        Returns:
            SparkDataFrame: The parsed data.
        """
        return (
            raw_df
            .select(
                F.col("key").cast("string").alias("seller"),
                F.from_json(F.col("value").cast("string"), self.schema).alias("data"),
                F.col("timestamp").alias("datetimezone"),
            )
            .select("seller", "datetimezone", "data.*")
        )

    def _write_bronze(self, df) -> StreamingQuery:
        """
        Writes raw data to the Bronze layer.

        Args:
            df (SparkSparkDataFrame): The raw data from Kafka.

        Returns:
            StreamingQuery: The streaming query that writes to the Bronze layer.
        """
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{config.BRONZE_LAYER_PATH}/_checkpoint")
            .start(config.BRONZE_LAYER_PATH)
        )

    def _write_silver(self, df) -> StreamingQuery:
        """
        Writes cleaned data to the Silver layer.

        Args:
            df (SparkSparkDataFrame): The DataFrame to write to the Silver layer.

        Returns:
            StreamingQuery: The streaming query that writes to the Silver layer.
        """
        def process_batch(batch_df, batch_id):
            if batch_df.isEmpty():
                return

            for seller in SELLER_CONFIGS.keys():
                seller_df = batch_df.filter(F.col("seller") == seller)
                if seller_df.isEmpty():
                    continue

                try:
                    logger.info(f"Processing Streaming Batch {batch_id} for {seller}")

                    clean_df = self._transform(seller, seller_df)
                    table_name = "STG_ALL_SELLERS_PRODUCTS"

                    
                    self.write_to_snowflake(clean_df, table_name, mode="append")

                except Exception as e:
                    logger.error(f"Failed streaming batch {batch_id} for {seller}: {e}")

        return (
            df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", f"{config.SILVER_LAYER_PATH}/_sf_checkpoint")
            .start()
        )
