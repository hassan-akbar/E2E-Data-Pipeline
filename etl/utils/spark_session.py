from pyspark.sql import SparkSession


def get_spark(app_name: str = "PosePipleine"):
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
