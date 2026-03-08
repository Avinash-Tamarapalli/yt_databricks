from pyspark.sql.types import *

metadata_schema = StructType([
    StructField("generated", LongType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("api", StringType(), True),
    StructField("count", IntegerType(), True),
    StructField("status", IntegerType(), True),
])

feature_geometry_schema = StructType([
    StructField("type", StringType(), True),
    StructField("coordinates", ArrayType(DoubleType()), True)
])

feature_properties_schema = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("updated", LongType(), True),
    StructField("tz", IntegerType(), True),
    StructField("url", StringType(), True),
    StructField("detail", StringType(), True),
    StructField("felt", IntegerType(), True),
    StructField("cdi", DoubleType(), True),
    StructField("mmi", DoubleType(), True),
    StructField("alert", StringType(), True),
    StructField("status", StringType(), True),
    StructField("tsunami", IntegerType(), True),
    StructField("sig", IntegerType(), True),
    StructField("net", StringType(), True),
    StructField("code", StringType(), True),
    StructField("ids", StringType(), True),
    StructField("sources", StringType(), True),
    StructField("types", StringType(), True),
    StructField("nst", IntegerType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("gap", DoubleType(), True),
    StructField("magType", StringType(), True),
    StructField("type", StringType(), True)
])

feature_schema = StructType([
    StructField("type", StringType(), True),
    StructField("properties", feature_properties_schema, True),
    StructField("geometry", feature_geometry_schema, True),
    StructField("id", StringType(), True)
])

earthquake_summary_schema = StructType([
    StructField("type", StringType(), True),
    StructField("metadata", metadata_schema, True),
    StructField("bbox", ArrayType(DoubleType()), True),
    StructField("features", ArrayType(feature_schema), True)
])