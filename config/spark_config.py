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
    ]
    conf.set("spark.jars.packages", ",".join(packages))
    
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.sql.parquet.vfilterPushdown", "true")

    return conf
