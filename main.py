import argparse
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from config.spark_config import spark_conf
from config.pipeline_config import SELLER_FILES, KAFKA_MESSAGE_SCHEMA

from jobs.batch import BatchPipeline
from jobs.streaming import StreamingPipeline
from logs.logger import get_logger


logger = get_logger()


def main():
    """
    Orchestrates the execution of the Earnest data engineering pipeline.

    This is the primary entry point for the application. It initializes the 
    Apache Spark session with Delta Lake support and triggers either the 
    batch processing engine or the real-time streaming consumer based 
    on command-line arguments.

    CLI Arguments:
        --mode (str): Execution mode. Options: 'batch' (process static files) 
                      or 'streaming' (consume from Kafka). Defaults to 'batch'.
        --skip-llm (flag): If present, bypasses LLM-based data imputation. 
                           Essential for error recovery or cost-saving scenarios.

    Raises:
        SystemExit: Exits with code 1 if:
            - An unsupported mode is provided.
            - Batch processing results in 0 records (indicating potential source failure).

    Example:
        $ python main.py --mode batch --skip-llm
        $ python main.py --mode streaming
    """

    parser = argparse.ArgumentParser(description="EarnestPipeline runner")
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch")
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        default=False,
        help="Skip LLM imputation entirely (use when all API keys are exhausted).",
    )
    args = parser.parse_args()
    mode = args.mode

    builder = SparkSession.builder.config(conf=spark_conf())
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger.info("SparkSession created — %s | mode: %s", spark.sparkContext.appName, mode)

    if mode == "batch":
        if args.skip_llm:
            logger.info("--skip-llm flag active: LLM imputation will be bypassed")
        pipeline = BatchPipeline(spark, skip_llm=args.skip_llm)
        processed = pipeline.run(SELLER_FILES)
        spark.stop()
        if processed == 0:
            logger.error("No sellers were processed. Exiting.")
            sys.exit(1)

    elif mode == "streaming":
        pipeline = StreamingPipeline(spark, schema=KAFKA_MESSAGE_SCHEMA)
        pipeline.run()

    else:
        logger.error("Unknown mode: %s. Use 'batch' or 'streaming'.", mode)
        spark.stop()
        sys.exit(1)

    logger.info("Pipeline finished — mode: %s", mode)


if __name__ == "__main__":
    main()