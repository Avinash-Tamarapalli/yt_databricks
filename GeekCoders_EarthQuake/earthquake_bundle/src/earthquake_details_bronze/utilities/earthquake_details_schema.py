from pyspark.sql.types import *



products_schema = StructType([
  StructField('id', StringType(), True),
  StructField('type', StringType(), True),
  StructField('code', StringType(), True),
  StructField('source', StringType(), True),
  StructField('updateTime', StringType(), True),
  StructField('status', StringType(), True),
  StructField('code', StringType(), True),
  StructField('source', StringType(), True)
])