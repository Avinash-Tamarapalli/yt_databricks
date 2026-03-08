from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    regexp_extract
)

from utilities.earthquake_summary_schema import earthquake_summary_schema

volume_path = "/Volumes/yt_dev/bronze_dev/earthquake_volume/summary"

# ----------------------------
# 1 Raw ingestion
# ----------------------------

@dp.view(
    name="earthquake_summary_raw_vw",
    comment="Raw ingestion of weekly earthquake summary JSON"
)
def earthquake_summary_raw():

    df = (
        spark.readStream
        .format("cloudFiles")
        .schema(earthquake_summary_schema)
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(volume_path)
        .withColumn("source_file", col("_metadata.file_path"))
    )

    return (
        df
        .withColumn(
            "week_start",
            regexp_extract(col("source_file"), r"week_start=([0-9]+)", 1)
        )
        .withColumn("ingestion_timestamp", current_timestamp())
    )


# ----------------------------
# 2 Flatten features
# ----------------------------

@dp.table(
    name="earthquake_weekly_summary_brnz_tbl",
    comment="Flattened earthquake features from summary feed"
)
def earthquake_summary_bronze():

    raw = spark.readStream.table("earthquake_summary_raw_vw")

    features = raw.select(
        explode(col("features")).alias("feature"),
        col("metadata"),
        col("week_start"),
        col("ingestion_timestamp"),
        col("source_file")
    )

    return (
        features.select(
            col("feature.id").alias("earthquake_id"),

            col("feature.properties.*"),

            col("feature.geometry.coordinates")[0].alias("longitude"),
            col("feature.geometry.coordinates")[1].alias("latitude"),
            col("feature.geometry.coordinates")[2].alias("depth"),

            col("metadata.generated").alias("metadata_generated"),
            col("metadata.url").alias("metadata_url"),
            col("metadata.title").alias("metadata_title"),
            col("metadata.api").alias("metadata_api"),
            col("metadata.count").alias("metadata_count"),
            col("metadata.status").alias("metadata_status"),

            col("week_start"),
            col("ingestion_timestamp"),
            col("source_file")
        )
    )