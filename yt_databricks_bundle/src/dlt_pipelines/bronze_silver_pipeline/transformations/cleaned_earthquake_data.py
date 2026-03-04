from pyspark.sql.functions import explode, col, current_timestamp
from transformations.earthquake_schemas import earthquake_raw_schema
import dlt

# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
catalog_name = spark.conf.get("catalog_name")

volume_path = f"/Volumes/{catalog_name}/bronze_dev/earthquake_volume"
primary_key = "id"

def earthquake_preprocess(df):
    df_processed = df.withColumn("time", (col("time") / 1000).cast("timestamp")) \
        .withColumn("updated", (col("updated") / 1000).cast("timestamp"))
    return df_processed

@dlt.view(name= 'earthquake_data_view')
def earthquake_data():
    earthquake_raw = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .option("cloudFiles.schemaEvolutionMode", "rescue") \
        .schema(earthquake_raw_schema) \
        .load(volume_path)
    
    earthquake_with_meta = earthquake_raw.select(
        col("_metadata.file_path").alias("source_file_path"),
        col("_metadata.file_name").alias("source_file_name"),
        col("_metadata.file_modification_time").alias("file_modification_time"),
        col("_rescued_data").alias("rescued_data"),
        current_timestamp().alias("ingestion_timestamp"),
        explode(col("features")).alias("features")
    )

    earthquake_data = earthquake_with_meta.select(
        "features.properties.*", 
        "features.id",
        col("features.geometry.coordinates")[0].alias("longitude"),
        col("features.geometry.coordinates")[1].alias("latitude"),
        col("features.geometry.coordinates")[2].alias("depth"),
        "ingestion_timestamp"
    )

    earthquake_data_view = earthquake_preprocess(earthquake_data)
    return earthquake_data_view

dlt.create_streaming_table(name= "earthquake_streaming_data")

dlt.apply_changes(
    target = "earthquake_streaming_data",
    source = "earthquake_data_view",
    keys = [primary_key],
    sequence_by = col("ingestion_timestamp"),
    stored_as_scd_type = 1
)





