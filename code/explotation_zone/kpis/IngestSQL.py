from pyspark.sql import SparkSession
import psycopg2
import os
import glob
from pyspark.sql.functions import col, when, median, count, isnull, round, count, mean, concat, avg, mode, \
    regexp_replace, udf
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.types import StringType


INCOME_PATH_HDFS = 'https://pidgeotto.fib.upc.es:9864/data/formatted_zone/income/income_formatted_zone'
IDEALISTA_INCIDENTS_PATH_HDFS = 'https://pidgeotto.fib.upc.es:9864/data/formatted_zone/idealista_incidents' \
                                '/idealista_incidents_formatted_zone '

# Create a SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("FormattedToSQL") \
    .getOrCreate()

rdd_income = spark.read.parquet(INCOME_PATH_HDFS)
table_income = rdd_income.select('year', 'id_neighborhood', 'population', 'RFD')
table_income1 = table_income.withColumn("year", when(col("year").isin([2016]), 2020).otherwise(col("year")))
table_income2 = table_income1.withColumn("year", when(col("year").isin([2017]), 2021).otherwise(col("year")))
table_income2 = table_income2.withColumn("year", table_income2["year"].cast("integer")) \
    .withColumn("id_neighborhood", table_income2["id_neighborhood"].cast("string")) \
    .withColumn("population", table_income2["population"].cast("integer")) \
    .withColumn("RFD", table_income2["RFD"].cast("double"))

rdd_idealista_incidents = spark.read.parquet(IDEALISTA_INCIDENTS_PATH_HDFS)
rdd_idealista_incidents1 = rdd_idealista_incidents.withColumn("yearmonth", concat(col("year"), col("month")))
table_price_incidents = rdd_idealista_incidents1.select('year', 'yearmonth', 'id_neighborhood', 'bathrooms', 'distance',
                                                        'exterior',
                                                        'floor', 'latitude', 'longitude', 'price', 'priceByArea',
                                                        'propertyType',
                                                        'rooms', 'size', 'status', 'incidents')

table_price_incidents = table_price_incidents.withColumn(
    "year", col("year").cast("int")).withColumn(
    "yearmonth", col("yearmonth").cast("int")).withColumn(
    "bathrooms", col("bathrooms").cast("int")).withColumn(
    "distance", col("distance").cast("int")).withColumn(
    "latitude", col("latitude").cast("float")).withColumn(
    "longitude", col("longitude").cast("float")).withColumn(
    "price", col("price").cast("float")).withColumn(
    "priceByArea", col("priceByArea").cast("float")).withColumn(
    "rooms", col("rooms").cast("int")).withColumn(
    "size", col("size").cast("float")).withColumn(
    "incidents", col("size").cast("int"))

table_price_incidents1 = table_price_incidents.groupBy("year", "yearmonth", "id_neighborhood").agg(
    avg("bathrooms").alias("bathrooms"),
    avg("distance").alias("distance"),
    mode("exterior").alias("exterior"),
    mode("floor").alias("floor"),
    avg("latitude").alias("latitude"),
    avg("longitude").alias("longitude"),
    avg("price").alias("price"),
    avg("priceByArea").alias("priceByArea"),
    mode("propertyType").alias("propertyType"),
    avg("rooms").alias("rooms"),
    avg("size").alias("size"),
    mode("status").alias("status"),
    avg("incidents").alias("incidents")
)

table_price_incidents1 = table_price_incidents1.withColumnRenamed("year", "year_id")

table_income2 = table_income2.withColumnRenamed("year", "year_id")
table_price_incidents1 = table_price_incidents1.withColumnRenamed("id_neighborhood", "neighborhood_id")
table_income2 = table_income2.withColumnRenamed("id_neighborhood", "neighborhood_id")

table_years = table_price_incidents1.select('year_id').distinct()
table_yearmonth = rdd_idealista_incidents1.select('yearmonth', 'month', 'year').distinct()
table_place = rdd_idealista_incidents1.select('id_neighborhood', 'neighborhood', 'district', 'city').distinct()
table_place = table_place.withColumnRenamed("neighborhood", "neighborhood_name")
table_place = table_place.withColumnRenamed("district", "district_name")
table_place = table_place.withColumnRenamed("id_neighborhood", "neighborhood_id")
table_yearmonth = table_yearmonth.withColumnRenamed("year", "year_id")
table_yearmonth = table_yearmonth.withColumnRenamed("month", "month_numb")


def get_month_name(month):
    month_names = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    return month_names[month - 1]


get_month_name_udf = udf(get_month_name, StringType())

table_tabletime = table_yearmonth.withColumn('month_name', get_month_name_udf(col('month_numb')))

print('table_price_incidents1')
print(table_price_incidents1.first())
print('table_income2')
print(table_income2.first())
print('table_years')
print(table_years.first())
print('table_tabletime')
print(table_tabletime.first())
print('table_place')
print(table_place.first())


def connect_postgresql(host, database, user, password):
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password)
    cursor = conn.cursor()
    return conn, cursor


def get_column_names_table(table_name, cursor):
    query = f"""SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';"""
    cursor.execute(query)
    return list(cursor.fetchall())


def insert_values_to_table(table_name, columns, values, cursor, conn):
    columns_string = ', '.join(columns)
    insert_query = f"INSERT INTO {table_name}({columns_string}) VALUES {values}"
    try:
        cursor.execute(insert_query)
    except Exception as e:
        print(e.args)
        print(values)
    # Commit the changes to the database
    conn.commit()


def insert_value_to_table(table_name, column, value, cursor, conn):
    insert_query = f"INSERT INTO {table_name}({column}) VALUES ({value})"
    try:
        cursor.execute(insert_query)
    except Exception as e:
        print(e.args)
        print(value)
    # Commit the changes to the database
    conn.commit()


def close_connection(cursor, conn):
    # Close the cursor and the connection
    cursor.close()
    conn.close()


con, cur = connect_postgresql(host='localhost',
                              database='bdm',
                              user='postgres',
                              password='postgres')

# table_years
# table_tabletime
# table_place
# table_price_incidents1
# table_income2


final_table_columns = table_years.columns[0]
for values in table_years.collect():
    insert_value_to_table(table_name='years',
                          column=final_table_columns,
                          value=str(values[0]),
                          conn=con,
                          cursor=cur)

final_table_columns = table_tabletime.columns
for values in table_tabletime.collect():
    insert_values_to_table(table_name='tabletime',
                           columns=final_table_columns,
                           values=tuple(values),
                           conn=con,
                           cursor=cur)

final_table_columns = table_place.columns
for values in table_place.collect():
    insert_values_to_table(table_name='place',
                           columns=final_table_columns,
                           values=tuple(values),
                           conn=con,
                           cursor=cur)

final_table_columns = table_income2.columns
for values in table_income2.collect():
    insert_values_to_table(table_name='income',
                           columns=final_table_columns,
                           values=tuple(values),
                           conn=con,
                           cursor=cur)

final_table_columns = table_price_incidents1.columns
for values in table_price_incidents1.collect():
    insert_values_to_table(table_name='price_incidents',
                           columns=final_table_columns,
                           values=tuple(values),
                           conn=con,
                           cursor=cur)

close_connection(con, cur)
spark.stop()
