# Using Medallion Architecture
# Bronze layer - > Silver Layer

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    to_timestamp,
    unix_timestamp,
    regexp_extract,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from utils.spark_session import get_spark


def clean_and_convert_to_parquet(input_path: str, output_path: str):

    spark = get_spark("PreprocessingJob")
    raw_count = 0

    # --- Define schema for consistency. ---
    p_schema = StructType(
        [
            StructField("patient_number", StringType(), False),
            StructField("start_time", StringType(), True),
            StructField("endtime", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("posture", StringType(), True),
        ]
    )

    # --- Load the csv ---
    print(f"Reading raw data from path : {input_path}")

    df_raw = spark.read.csv(input_path, header=True, schema=p_schema)
    raw_count = df_raw.count()
    # -- Data Cleaning --

    # Normalise data types

    df = (
        df_raw.withColumn("patient_number", trim(col("patient_number")))
        .withColumn("start_time", to_timestamp(col("start_time"), "d/MM/yyyy H:mm"))
        .withColumn("end_time", to_timestamp(col("endtime"), "d/MM/yyyy H:mm"))
        .withColumn("posture", trim(upper(col("posture"))))
        .drop("endtime")
    )

    # Calculate duration based on timestamps
    # Although duration is given I still prefer this overhead as raw data can be corrupted.
    # Additionally, this is a runtime metric that can be calculated.

    df = df.withColumn(
        "duration",
        when(
            col("start_time").isNotNull() & col("end_time").isNotNull(),
            unix_timestamp(col("end_time")) - unix_timestamp(col("start_time")),
        ).otherwise(None),
    )

    # --- Split composite key into ward and bed number ---
    df = (
        df.withColumn(
            "ward", regexp_extract(col("patient_number"), r"Ward([A-Za-z]+)-Bed", 1)
        )
        .withColumn(
            "bed_number", regexp_extract(col("patient_number"), r"Bed([0-9A-Za-z]+)", 1)
        )
        .drop("patient_number")
    )

    # --- Normalize posture categories ---

    valid_postures = ["SUPINE", "LEFT", "RIGHT", "PRONE", "NPP", "NO DATA"]
    df = df.withColumn(
        "posture",
        when(col("posture").isin(valid_postures), col("posture")).otherwise("UNKNOWN"),
    )

    # --- Drop invalid or incomplete rows ---
    valid_condition = (
        (col("ward").isNotNull())
        & (col("bed_number").isNotNull())
        & (col("start_time").isNotNull())
        & (col("end_time").isNotNull())
        & (col("duration").isNotNull())
        & (col("duration") >= 0)
        & (col("posture").isNotNull())
        & (col("start_time") < col("end_time"))
    )

    df_cleaned = df.filter(valid_condition)
    df_dropped = df.filter(~valid_condition)
    cleaned_count = df_cleaned.count()
    dropped_count = raw_count - cleaned_count
    drop_rate = (dropped_count / raw_count * 100) if raw_count > 0 else 0

    print(f"📊 Raw rows: {raw_count}")
    print(f"✅ Cleaned rows: {cleaned_count}")
    print(f"⚠️ Dropped rows: {dropped_count} ({drop_rate:.2f}% dropped)")

    # --- Sort and write Parquet ---
    df_cleaned = df_cleaned.orderBy(["ward", "bed_number", "start_time"])
    print(f"💾 Writing cleaned Parquet to: {output_path}")
    df_cleaned.write.mode("overwrite").parquet(output_path)

    # --- Optionally show dropped rows ---
    print("🔍 Sample of dropped rows:")
    df_dropped.show(10, truncate=False)

    print("🏁 Preprocessing complete.")
    spark.stop()


if __name__ == "__main__":

    INPUT_PATH = "./data/raw/raw_data.csv"
    OUTPUT_PATH = "./data/curated/pose_data_cleaned"
    clean_and_convert_to_parquet(INPUT_PATH, OUTPUT_PATH)
