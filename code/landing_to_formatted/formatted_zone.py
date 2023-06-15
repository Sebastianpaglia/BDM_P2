from pyspark.sql import SparkSession
import os
import glob
from mongo_connector import load_collection


def list_to_rdd(spark_connector, list_documents):
    return spark_connector.sparkContext.parallelize(list_documents)


def add_columns(row, value: tuple):
    return row + value


def read_idealista_landing_zone(spark_connector):
    idealista_selected_columns = ['district', 'neighborhood', 'municipality', 'bathrooms',
                                  'distance', 'exterior', 'floor', 'latitude',
                                  'longitude', 'price', 'priceByArea', 'propertyType', 'rooms', 'size', 'status']
    data_idealista_path = 'data/landing_zone/idealista'
    date_folders = os.listdir(data_idealista_path)
    idealista_rdd = spark_connector.sparkContext.parallelize([])
    for folder in date_folders[:10]:
        search_pattern = os.path.join(data_idealista_path, '**/*.parquet')
        parquet_file = glob.glob(search_pattern, recursive=True)
        idealista_file_df = spark_connector.read.parquet(*parquet_file).select(idealista_selected_columns)
        columns = idealista_file_df.columns + ['year', 'month']
        idealista_file_rdd = idealista_file_df.rdd
        year_month = tuple([int(elem) for elem in folder.split('_')[:2]])
        idealista_file_rdd = idealista_file_rdd.map(lambda x: add_columns(x, year_month))
        idealista_rdd = idealista_rdd.union(idealista_file_rdd)

    return idealista_rdd


def read_incidents_landing_zone(spark_connector):
    data_incidents_path = 'data/landing_zone/incidents'
    incidents_rdd = spark_connector.read.options(header='True', inferSchema='True', delimiter=',') \
        .csv(data_incidents_path).rdd
    return incidents_rdd


def prepare_income_formatted_zone(income_rdd_input, income_neigh_lookup):
    return income_rdd_input \
        .flatMap(lambda x: [{"neigh_name": x["neigh_name "],
                             "district_name": x["district_name"],
                             "year": d["year"],
                             "pop": d["pop"],
                             "RFD": d["RFD"]} for d in x["info"]]) \
        .keyBy(lambda x: x['neigh_name']) \
        .join(income_neigh_lookup.keyBy(lambda x: x['neighborhood'])) \
        .map(lambda x: (x[1][1]['neighborhood_reconciled'],
                        x[1][1]["_id"], x[1][0]['district_name'],
                        x[1][0]['year'],
                        x[1][0]['pop'],
                        x[1][0]['RFD']))


def prepare_idealista_formatted_zone(idealista_rdd_input, idealista_neigh_lookup):
    return idealista_rdd_input.keyBy(lambda x: x[1]) \
        .join(idealista_neigh_lookup.keyBy(lambda x: x["ne"])) \
        .map(lambda x: (x[1][0][0],) + (x[1][1]["_id"], x[1][1]["ne_re"]) + x[1][0][2:])


def prepare_incidents_formatted_zone(incidents_rdd_input, incidents_neigh_lookup):
    return incidents_rdd_input \
        .groupBy(lambda x: x[2:9]) \
        .mapValues(lambda x: sum(int(v[9]) for v in x if v[9] is not None)) \
        .map(lambda x: x[0] + (x[1],)) \
        .keyBy(lambda x: x[3]) \
        .join(incidents_neigh_lookup.keyBy(lambda x: x['neighborhood'])) \
        .map(lambda x: (x[1][1]["neighborhood_reconciled"], x[1][1]["_id"]) + tuple(x[1][0])[:3] + tuple(x[1][0])[4:])


spark = SparkSession.builder \
    .master(f"local[*]") \
    .appName("LandingToFormatted") \
    .getOrCreate()

# read data
idealista_rdd = read_idealista_landing_zone(spark)
incidnets_rdd = read_incidents_landing_zone(spark)
income_rdd = list_to_rdd(spark, load_collection('income'))
income_neigh = list_to_rdd(spark, load_collection('income_lookup_neighborhood'))
rent_neigh = list_to_rdd(spark, load_collection('rent_lookup_neighborhood'))

income_conciled = prepare_income_formatted_zone(income_rdd_input=income_rdd,
                                                income_neigh_lookup=income_neigh)

idealista_conciled = prepare_idealista_formatted_zone(idealista_rdd_input=idealista_rdd,
                                                      idealista_neigh_lookup=rent_neigh)

incidents_conciled = prepare_incidents_formatted_zone(incidents_rdd_input=incidnets_rdd,

                                                      incidents_neigh_lookup=income_neigh)

idealista_incidents_conciled = idealista_conciled.map(lambda x: (x[1] + "_" + str(x[16]) + "_" + str(x[17]), x)) \
    .join(incidents_conciled.map(lambda x: (x[1] + "_" + x[5] + "_" + x[6], x))) \
    .map(lambda x: x[1][0] + (x[1][1][8],))

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

df_idealista_incidents = idealista_incidents_conciled.toDF(columns_idealista_incidents)
df_idealista_incidents.write.parquet('data/formatted_zone/idealista_incidents_formatted_zone')

columns_income = ['neighborhood',
                  'id_neighborhood',
                  'district',
                  'year',
                  'population',
                  'RFD']

df_income = income_conciled.toDF(columns_income)

df_income.write.parquet('data/formatted_zone/income_formatted_zone')
# Stop the SparkSession
spark.stop()
