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
    .appName("LandingToFormatted") \
    .getOrCreate()

idealista_selected_columns = ['district', 'neighborhood', 'municipality', 'bathrooms',
                              'distance', 'exterior', 'floor', 'latitude',
                              'longitude', 'price', 'priceByArea', 'propertyType', 'rooms', 'size', 'status']

data_idealista_path = 'data/idealista'
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

data_incidents_path = 'data/incidents'
incidents_rdd = spark.read.options(header='True', inferSchema='True', delimiter=',') \
    .csv(data_incidents_path).rdd

# Convert DataFrame to RDD
income_rdd = list_to_rdd(spark, load_collection('income'))
income_neigh = list_to_rdd(spark, load_collection('income_lookup_neighborhood'))
rent_neigh = list_to_rdd(spark, load_collection('rent_lookup_neighborhood'))

"""income_rdd_by_key = income_rdd.flatMap(lambda x:
                   [{"neigh_name": x["neigh_name "],
                     "district_name": x["district_name"],
                     "year": d["year"],
                     "pop": d["pop"],
                     "RFD": d["RFD"]}
                    for d in x["info"]]).keyBy(lambda x: x['neigh_name'])

income_neigh_by_key = income_neigh.keyBy(lambda x: x['neighborhood'])

income_joined = income_rdd_by_key.join(income_neigh_by_key)
income_conciled = income_joined.map(lambda x: (
                                x[1][1]['neighborhood_reconciled'],
                                x[1][1]["_id"], x[1][0]['district_name'],
                                x[1][0]['year'],
                                x[1][0]['pop'],
                                x[1][0]['RFD']
                            )
                  )

columns_income = ['neighborhood',
                  'id_neighborhood',
                  'district',
                  'year',
                  'population',
                  'RFD']

df_income = income_conciled.toDF(columns_income)
df_income.show()"""
# df_income.write.parquet('income_formatted_zone')

# Joining idealista with lookup
rent_neigh_bykey = rent_neigh.keyBy(lambda x: x["ne"])
idealista_rdd_bykey = idealista_rdd.keyBy(lambda x: x[1])
idealista_rdd_joined = idealista_rdd_bykey.join(rent_neigh_bykey)
idealista_conciled = idealista_rdd_joined.map(
    lambda x: (x[1][0][0],) + (x[1][1]["_id"], x[1][1]["ne_re"]) + x[1][0][2:])

incidents_rdd_by_key = incidents_rdd \
    .groupBy(lambda x: x[2:9]) \
    .mapValues(lambda x: sum(int(v[9]) for v in x if v[9] is not None)) \
    .map(lambda x: x[0] + (x[1],)) \
    .keyBy(lambda x: x[3])

income_neigh_by_key = income_neigh.keyBy(lambda x: x['neighborhood'])

incidents_rdd_joined = incidents_rdd_by_key.join(income_neigh_by_key)
incidents_conciled = incidents_rdd_joined.map(
    lambda x: (x[1][1]["neighborhood_reconciled"], x[1][1]["_id"]) + tuple(x[1][0])[:3] + tuple(x[1][0])[4:])

idealista_conciled_bykey = idealista_conciled.map(lambda x: (x[1] + "_" + str(x[16]) + "_" + str(x[17]), x))
incidents_conciled_bykey = incidents_conciled.map(lambda x: (x[1] + "_" + x[5] + "_" + x[6], x))
rdd1 = idealista_conciled_bykey.join(incidents_conciled_bykey)
rdd1_final = rdd1.map(lambda x: x[1][0] + (x[1][1][8], ))
columns_idealista_incidents = ['district',
                               'id_neighborhood',
                               'neighborhood',
                               'city',
                               'bathrooms',
                               'distance',
                               'exterior',
                               'floor', 'latitude',
                               'longitude', 'price', 'priceByArea', 'propertyType', 'rooms',
                               'size', 'status', 'year', 'month', 'incidents']

df_idealista_incidents = rdd1_final.toDF(columns_idealista_incidents)
df_idealista_incidents.write.parquet('idealista_incidents_formatted_zone')
# Stop the SparkSession
spark.stop()
