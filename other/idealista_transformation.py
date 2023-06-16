from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time
from pymongo import MongoClient
import os
import glob

"""spark = SparkSession \
    .builder \
    .master(f"local[*]") \
    .appName("myApp") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()

incomeRDD = spark.read.format("mongo") \
    .option('uri', f"mongodb://localhost/test.test_bdm") \
    .load() \
    .rdd"""


def load_collection(collection):
    # Connect to MongoDB
    client = MongoClient("mongodb://10.4.41.48")

    # Access the desired database and collection
    db = client["landing_zone"]
    documents_collection = db[collection]
    documents = list(documents_collection.find())
    return documents


def list_to_rdd(spark_connector, list_documents):
    return spark_connector.sparkContext.parallelize(list_documents)


def add_columns(row, value: tuple):
    return row + value


spark = SparkSession.builder \
    .master(f"local[*]") \
    .appName("List to RDD") \
    .getOrCreate()

idealista_selected_columns = ['district', 'neighborhood', 'municipality', 'bathrooms',
                              'distance', 'exterior', 'floor', 'latitude',
                              'longitude', 'price', 'priceByArea', 'propertyType', 'rooms', 'size', 'status']
data_idealista_path = '../data/landing_zone/idealista'
date_folders = os.listdir(data_idealista_path)
idealista_rdd = spark.sparkContext.parallelize([])
for folder in date_folders[:10]:
    search_pattern = os.path.join(data_idealista_path, '**/*.parquet')
    parquet_file = glob.glob(search_pattern, recursive=True)
    idealista_file_df = spark.read.parquet(*parquet_file).select(idealista_selected_columns)
    columns = idealista_file_df.columns + ['year', 'month']
    idealista_file_rdd = idealista_file_df.rdd
    year_month = tuple([int(elem) for elem in folder.split('_')[:2]])
    idealista_file_rdd = idealista_file_rdd.map(lambda x: add_columns(x, year_month))
    idealista_rdd = idealista_rdd.union(idealista_file_rdd)

idealista_rdd.toDF(columns).show()

data_incidents_path = '../data/landing_zone/incidents'
incidents_rdd = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv(data_incidents_path)
incidents_rdd.show()

# Convert DataFrame to RDD
income_rdd = list_to_rdd(spark, load_collection('income'))
income_district = list_to_rdd(spark, load_collection('income_lookup_district'))
income_neigh = list_to_rdd(spark, load_collection('income_lookup_neighborhood'))
rent_district = list_to_rdd(spark, load_collection('rent_lookup_district'))
rent_neigh = list_to_rdd(spark, load_collection('rent_lookup_neighborhood'))


rent_neigh_bykey = rent_neigh.keyBy(lambda x: x["ne"])
idealista_rdd_bykey = idealista_rdd.keyBy(lambda x: x[1])

idealista_rdd_joined = idealista_rdd_bykey.join(rent_neigh_bykey)
idealista_rdd_joined.toDF().show()
idealista_conciled = idealista_rdd_joined.map(lambda x: (x[1][0][0],)+(x[1][1]["_id"],x[1][1]["ne_re"])+x[1][0][2:])



# Stop the SparkSession
spark.stop()