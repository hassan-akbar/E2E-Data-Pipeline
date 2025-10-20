# Using Medallion Architecture: Silver -> Gold Layer
# Computes posture %, continuous on-bed metrics, and disconnection frequency.

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    when,
    round,
    avg,
    max as _max,
    lag,
    min as _min,
    unix_timestamp,
)

from utils.spark_session import get_spark


def aggregate_patient_metrics(input_path: str, output_path: str):
    spark = get_spark("GoldAggregationJob")

    print(f"Reading file from path {input_path}")

    df = spark.read.parquet(input_path)

    # -- Position % spent in each posture --

    total_time_df = df.groupBy("ward", "bed_number").agg(
        _sum("duration").alias("total_duration")
    )

    posture_pct_df = (
        df.groupBy("ward", "bed_number", "posture")
        .agg(_sum("duration").alias("posture_duration"))
        .join(total_time_df, ["ward", "bed_number"], "left")
        .withColumn(
            "posture_pct",
            round(col("posture_duration") / col("total_duration") * 100, 2),
        )
        .groupBy("ward", "bed_number")
        .pivot("posture", ["LEFT", "RIGHT", "PRONE", "SUPINE", "NPP", "NO DATA"])
        .agg(round(_max("posture_pct"), 2))
        .fillna(0)
    )

    # --- Calculate Average Hours spent in continous in bed. ---

    # Filter the data to only get information about valid postures
    w = Window.partitionBy("ward", "bed_number").orderBy("start_time")
    df_inbed = df.filter(~col("posture").isin(["NPP", "NO_DATA"]))

    # Get previous end times of each entry
    df_seq = df_inbed.withColumn("prev_end_time", lag("end_time").over(w))

    # Flag if there is a gap between enteries
    df_seq = df_seq.withColumn(
        "new_streak",
        when(
            col("prev_end_time").isNull() | (col("start_time") > col("prev_end_time")),
            1,
        ).otherwise(0),
    )

    # Check total duration spent within a streak
    w2 = (
        Window.partitionBy("ward", "bed_number")
        .orderBy("start_time")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    df_seq = df_seq.withColumn("streak_id", _sum("new_streak").over(w2))

    # Aggregate data based on streak_id and summate the duration per streak_id
    streaks = df_seq.groupBy("ward", "bed_number", "streak_id").agg(
        _sum("duration").alias("streak_duration")
    )

    # Get the average
    risk_df = streaks.groupBy("ward", "bed_number").agg(
        round(avg("streak_duration") / 3600, 2).alias("continous_on_bed_hrs")
    )

    # --- Freequency of NO DATA ---
    disconnection_df = (
        df.filter(col("posture") == "NO DATA")
        .groupBy("ward", "bed_number")
        .agg(count("*").alias("no_data_count"))
    )

    # --- Posture Variability ---

    w = Window.partitionBy("ward", "bed_number").orderBy("start_time")

    df_variation = (
        df.withColumn("prev_posture", lag("posture").over(w))
        .withColumn(
            "changed", when(col("posture") != col("prev_posture"), 1).otherwise(0)
        )
        .groupBy("ward", "bed_number")
        .agg(
            count(when(col("changed") == 1, True)).alias("posture_changes"),
            _min("start_time").alias("first_start"),
            _max("end_time").alias("last_end"),
        )
        .withColumn(
            "total_observed_hours",
            (unix_timestamp("last_end") - unix_timestamp("first_start")) / 3600,
        )
        .withColumn(
            "posture_changes_per_hour",
            when(
                col("total_observed_hours") > 0,
                round(col("posture_changes") / col("total_observed_hours"), 2),
            ).otherwise(0),
        )
        .select("ward", "bed_number", "posture_changes", "posture_changes_per_hour")
    )

    final_df = (
        posture_pct_df.join(risk_df, ["ward", "bed_number"], "left")
        .join(disconnection_df, ["ward", "bed_number"], "left")
        .join(df_variation, ["ward", "bed_number"], "left")
        .fillna(0)
    )

    for colname in ["LEFT", "RIGHT", "PRONE", "SUPINE", "NPP", "NO DATA"]:

        final_df = final_df.withColumnRenamed(
            colname, f"pct_{colname.lower().replace(' ', '_')}"
        )

    print(f"💾 Writing aggregated Gold metrics to: {output_path}")
    final_df.write.mode("overwrite").parquet(output_path)

    print("🏁 Gold Layer aggregation complete.")
    spark.stop()


if __name__ == "__main__":
    INPUT_PATH = "./data/curated/pose_data_cleaned"
    OUTPUT_PATH = "./data/analytics/pose_data_metrics"
    aggregate_patient_metrics(INPUT_PATH, OUTPUT_PATH)
