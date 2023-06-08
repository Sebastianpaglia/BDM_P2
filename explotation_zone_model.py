from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
from pymongo import MongoClient
import os
import glob


spark = SparkSession.builder \
    .master(f"local[*]") \
    .appName("LandingToFormatted") \
    .getOrCreate()


def read_directory_parquet_files(directory, spark):
    search_pattern = os.path.join(directory, '**/*.parquet')
    parquet_file = glob.glob(search_pattern, recursive=True)
    rdd = spark.read.parquet(*parquet_file).rdd
    return rdd


rdd_idealista_incidents = read_directory_parquet_files('data/formatted_zone/idealista_incidents_formatted_zone', spark)

rdd_income = read_directory_parquet_files('data/formatted_zone/income_formatted_zone', spark)


print(rdd_idealista_incidents.first())

print(rdd_income.first())





