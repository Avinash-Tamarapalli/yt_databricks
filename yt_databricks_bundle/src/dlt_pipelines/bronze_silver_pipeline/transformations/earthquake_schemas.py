from pyspark.sql.types import *

earthquake_metadata_schema = StructType([
        StructField("generated", LongType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("api", StringType(), True),
        StructField("count", IntegerType(), True)
    ])

earthquake_feature_properties_schema = StructType([
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
                StructField("type", StringType(), True),
                StructField("title", StringType(), True)
            ])

earthquake_feature_geometry_schema = StructType([
                StructField("type", StringType(), True),
                StructField("coordinates",
                    ArrayType(DoubleType()), True
                )
            ])

earthquake_each_feature_schema = StructType([
        StructField("type", StringType(), True),
        StructField("properties", earthquake_feature_properties_schema, True),
        StructField("geometry", earthquake_feature_geometry_schema, True),
        StructField("id", StringType(), True)
])

earthquake_raw_schema = StructType([

    StructField("type", StringType(), True),

    StructField("metadata", earthquake_metadata_schema , True),

    StructField("features", ArrayType(earthquake_each_feature_schema), True),

    StructField("bbox", ArrayType(DoubleType()), True)

])