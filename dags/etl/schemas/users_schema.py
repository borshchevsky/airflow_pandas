from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('email', StringType()),
])
