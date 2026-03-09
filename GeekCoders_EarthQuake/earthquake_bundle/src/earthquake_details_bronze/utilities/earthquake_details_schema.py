from pyspark.sql.types import *

# Use MapType for contents because the keys (filenames) vary
contents_schema = MapType(
    StringType(),
    StructType([
        StructField("contentType", StringType(), True),
        StructField("lastModified", LongType(), True),
        StructField("length", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("sha256", StringType(), True)
    ])
)

# Shared schema for origin, phase-data, and nearby-cities
products_item_schema = StructType([
    StructField("indexid", StringType(), True),
    StructField("indexTime", LongType(), True),
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("code", StringType(), True),
    StructField("source", StringType(), True),
    StructField("updateTime", LongType(), True),
    StructField("status", StringType(), True),
    StructField("properties", MapType(StringType(), StringType()), True), # Properties are often dynamic strings
    StructField("preferredWeight", IntegerType(), True),
    StructField("contents", contents_schema, True)
])

products_schema = StructType([
    StructField('nearby-cities', ArrayType(products_item_schema), True),
    StructField('origin', ArrayType(products_item_schema), True),
    StructField('phase-data', ArrayType(products_item_schema), True)
])

# Inside properties, 'products' is nested
details_properties_schema = StructType([
    StructField("mag", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("updated", LongType(), True),
    StructField("tz", IntegerType(), True),
    StructField("url", StringType(), True),
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
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("products", products_schema, True) # Fixed the variable name here
])

earthquake_details_schema = StructType([
    StructField('type', StringType(), True),
    StructField('properties', details_properties_schema, True),
    StructField('geometry', StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ]), True),
    StructField('id', StringType(), True)
])