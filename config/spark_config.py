from pyspark import SparkConf


def spark_conf() -> SparkConf:
    """
    Returns a configured SparkConf object for the EarnestSparkApp.
    """
    conf = SparkConf()
    conf.setAppName("EarnestSparkApp")
    conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    conf.set("spark.sql.ansi.enabled", "false")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
    conf.set("spark.driver.extraJavaOptions", "-Dhadoop.security.authentication.use.subject.current=false")
    conf.set("spark.executor.extraJavaOptions", "-Dhadoop.security.authentication.use.subject.current=false")

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        "org.apache.spark:spark-avro_2.13:4.0.0",
        "org.apache.hadoop:hadoop-azure:3.3.6",
    ]
    conf.set("spark.jars.packages", ",".join(packages))

    from config.dotenv_config import config
    if config.AZURE_STORAGE_ACCOUNT_NAME and config.AZURE_STORAGE_ACCOUNT_KEY:
        conf.set(f"fs.azure.account.key.{config.AZURE_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net", config.AZURE_STORAGE_ACCOUNT_KEY)
        conf.set(f"fs.azure.account.key.{config.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net", config.AZURE_STORAGE_ACCOUNT_KEY)

    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.sql.parquet.vfilterPushdown", "true")
    return conf