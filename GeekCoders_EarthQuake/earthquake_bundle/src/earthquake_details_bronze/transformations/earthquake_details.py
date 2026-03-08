from pyspark import pipelines as dp
# from utilities.earthquake_details_schema import earthquake_details_schema

from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    regexp_extract
)

volume_path = "/Volumes/yt_dev/bronze_dev/earthquake_volume/details"

@dp.view(
    name="earthquake_details_vw",
    comment="This is a view of earthquake details"
)
def earthquake_details_vw():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.inferSchema", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(volume_path)
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("latest_modified_time", col("_metadata.file_modification_time"))

    )
    return df


@dp.table(
    name="earthquake_details_brnz_tbl",
    comment="This is a table of earthquake details",
)
def earthquake_details_tbl():

    details = spark.readStream.table("earthquake_details_vw")
    return details