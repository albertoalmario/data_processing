from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# SparkSession creation
spark = SparkSession.builder.appName("globant_challenge")\
    .config('spark.master','local[4]')\
    .getOrCreate()

# spark.read.csv('')