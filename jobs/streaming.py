import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.avro.functions import from_avro
from .BasePipeline import BasePipeline
from .avro_schema import STREAMING_AVRO_SCHEMA_JSON

from config.dotenv_config import config

from config.pipeline_config import SELLER_CONFIGS, _DEFAULTS 

from logs.logger import get_logger

logger = get_logger()


class StreamingPipeline(BasePipeline):
    """
    Orchestrates the Kafka → Spark Structured Streaming pipeline.
    Kafka (Avro) -> Transform -> Silver
    """

    def __init__(self, spark: SparkSession, schema: StructType = None, skip_llm: bool = False):
        super().__init__(spark)
        self.schema = schema 
        self.skip_llm = skip_llm

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

        self._monitor_streams(idle_timeout_minutes=10)

    def _monitor_streams(self, idle_timeout_minutes: int) -> None:
        """
        Monitors active streams and stops them safely.
        Will NOT stop if streams are actively processing a heavy batch (like LLM).
        """
        timeout_seconds = idle_timeout_minutes * 60
        last_active_time = time.time()
        
        logger.info(f"Monitoring streams... Will shut down automatically if fully idle for {idle_timeout_minutes} minutes.")

        while True:
            active_queries = self.spark.streams.active
            
            if not active_queries:
                logger.info("All streaming queries have naturally stopped.")
                break

            is_actively_processing = False
            any_data_received = False

            for query in active_queries:
                if query.status.get('isTriggerActive', False):
                    is_actively_processing = True

                progress = query.lastProgress
                if progress and progress.get("numInputRows", 0) > 0:
                    any_data_received = True

            if is_actively_processing or any_data_received:
                last_active_time = time.time()
            else:
                idle_duration = time.time() - last_active_time
                if idle_duration > timeout_seconds:
                    logger.warning(f"No new data and completely idle for {idle_timeout_minutes} minutes. Stopping streams gracefully...")
                    for query in active_queries:
                        query.stop()
                    break 

            time.sleep(30)

    def _read_kafka(self) -> SparkDataFrame:
        """
        Reads raw data from Kafka topic using credentials from the environment config.
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
            .option("startingOffsets", "earliest")
            .option("kafka.max.poll.interval.ms", "1800000")
            .load()
        )

    def _parse(self, raw_df) -> SparkDataFrame:
        """
        Parses the raw Avro data from Kafka.
        """
        return (
            raw_df
            .select(
                F.col("key").cast("string").alias("seller"),
                from_avro(F.col("value"), STREAMING_AVRO_SCHEMA_JSON).alias("data"),
                F.col("timestamp").alias("datetimezone"),
            )
            .select("seller", "datetimezone", "data.*")
        )

    def _write_bronze(self, df) -> StreamingQuery:
        """
        Writes raw data to the Bronze layer every 3 minutes.
        """
        return (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .trigger(processingTime='3 minutes') 
            .option("checkpointLocation", f"{config.BRONZE_LAYER_PATH}/_checkpoint")
            .start(config.BRONZE_LAYER_PATH)
        )

    def _write_silver(self, df) -> StreamingQuery:
        """
        Writes cleaned data to the Silver layer every 3 minutes, handles dynamic sellers.
        """
        def process_batch(batch_df, batch_id):
            if batch_df.isEmpty():
                return

            batch_df.persist()

            try:
                total_rows = batch_df.count()
                logger.info(f"\n{'='*50}\nStarted Batch {batch_id}: Received {total_rows} total rows\n{'='*50}")

                distinct_rows = batch_df.select("seller").distinct().collect()
                distinct_sellers = [row.seller for row in distinct_rows if row.seller]

                for seller in distinct_sellers:
                    seller_df = batch_df.filter(F.col("seller") == seller)
                    
                    seller_rows = seller_df.count()
                    if seller_rows == 0:
                        continue

                    if seller not in SELLER_CONFIGS:
                        logger.warning(f"Unknown seller detected: '{seller}'. Using default configurations.")
                        SELLER_CONFIGS[seller] = _DEFAULTS.copy()

                    logger.info(f"Found {seller_rows} rows for '{seller}'.")
                    logger.info(f"Sample data for {seller}:")
                    seller_df.show(3, truncate=False)

                    logger.info(f"Applying transformations for {seller}...")
                    clean_df = self._transform(seller, seller_df, skip_llm=self.skip_llm)
                    
                    table_name = "STG_ALL_SELLERS_PRODUCTS"
                    logger.info(f"Writing {seller_rows} rows to Snowflake table: {table_name}...")
                    
                    self.write_to_snowflake(clean_df, table_name, mode="append")
                    logger.info(f"Successfully finished processing {seller} for Batch {batch_id}\n{'-'*50}")

            except Exception as e:
                logger.error(f"Failed streaming batch {batch_id}: {e}\n{'-'*50}")
                
            finally:
                batch_df.unpersist()

        return (
            df.writeStream
            .foreachBatch(process_batch)
            .trigger(processingTime='3 minutes')
            .option("checkpointLocation", f"{config.BRONZE_LAYER_PATH}/_sf_checkpoint")
            .start()
        )