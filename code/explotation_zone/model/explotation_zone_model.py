from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, median, count, isnull, round, count, mean
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor, DecisionTreeRegressor, GBTRegressor
import psycopg2
from pyspark.sql.functions import regexp_replace
from pyspark.ml.util import MLWriter
from datetime import datetime

INCOME_PATH_HDFS = 'https://pidgeotto.fib.upc.es:9864/data/formatted_zone/income/income_formatted_zone'
IDEALISTA_INCIDENTS_PATH_HDFS = 'https://pidgeotto.fib.upc.es:9864/data/formatted_zone/idealista_incidents/idealista_incidents_formatted_zone'
MODEL_HDFS = "hdfs://pidgeotto.fib.upc.es:9864/models"

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
    cursor.execute(insert_query)
    # Commit the changes to the database
    conn.commit()


def close_connection(cursor, conn):
    # Close the cursor and the connection
    cursor.close()
    conn.close()


def read_directory_parquet_files(directory, spark):
    """search_pattern = os.path.join(directory, '**/*.parquet')
    parquet_file = glob.glob(search_pattern, recursive=True)"""
    rdd = spark.read.parquet(*directory).rdd
    return rdd


def build_dataset(spark):
    rdd_idealista_incidents = read_directory_parquet_files(IDEALISTA_INCIDENTS_PATH_HDFS,
                                                           spark)

    rdd_idealista_incidents.map(lambda x: x[:5] + (int(x[5]),) + (x[6],) + (int(x[7]),) + x[8:]).first()
    rdd_income = read_directory_parquet_files(INCOME_PATH_HDFS, spark)

    rdd_income_mapping = rdd_income.map(
        lambda x: (2020 if x[3] == 2016 else (2021 if x[3] == 2017 else x[3]),) + x[:3] + x[4:])

    rdd1_pairs = rdd_idealista_incidents.map(lambda x: ((x[1], x[16]), x))

    rdd2_pairs = rdd_income_mapping.map(lambda x: ((x[2], x[0]), x))

    # Join the RDDs based on the common keys
    joined_rdd = rdd1_pairs.join(rdd2_pairs)

    columns_final_dataset = ['district', 'id_neighborhood', 'neighborhood', 'city',
                             'bathrooms', 'distance', 'exterior', 'floor', 'latitude', 'longitude', 'price',
                             'priceByArea',
                             'propertyType', 'rooms', 'size', 'status', 'year', 'month', 'incidents', 'population',
                             'RFD']

    rdd_dataset = joined_rdd.map(lambda x: tuple(x[1][0]) + (x[1][1][4], x[1][1][5]))

    df = rdd_dataset.toDF(columns_final_dataset).distinct()

    df = df.withColumn('floor',
                       when(col('floor').cast('double') > 0, col('floor') + 2)
                       .when(col('floor').cast('double') < 0, col('floor'))
                       .when(col('floor') == 'bj', 0)
                       .when(col('floor') == 'en', 1)
                       .when(col('floor') == 'st', 2)
                       .otherwise(col('floor')))

    df = df.withColumn('exterior', when(col('exterior'), 1).otherwise(0))

    return df


def impute_floor_varible(df, median_values):
    return df.join(median_values, on='id_neighborhood', how='left') \
        .withColumn('floor', when(col('floor').isNull(), col('round(median(floor), 0)')).otherwise(col('floor'))) \
        .drop('round(median(floor), 0)').withColumn('floor', col('floor').cast('integer')).withColumn('distance',
                                                                                                      col('distance').cast(
                                                                                                          'integer'))


def build_pipeline(model):
    # Create StringIndexer stages for each categorical column
    indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid="keep") for col in
                categorical_columns]

    # Apply OneHotEncoder to the indexed columns
    encoders = [OneHotEncoder(inputCol=col + "_index", outputCol=col + "_encoded") for col in categorical_columns]

    assembler = VectorAssembler(inputCols=[col + "_encoded" for col in categorical_columns] + numerical_columns,
                                outputCol="features")

    pipeline = Pipeline(stages=indexers + encoders + [assembler, model])

    return pipeline


def save_model_and_datasets(model, train_data, test_data, model_path, spark):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    writer = MLWriter()
    writer.save(model, f"{model_path}/model_{timestamp}")
    train_data.write.parquet(f"{model_path}/train_data_{timestamp}.parquet")
    test_data.write.parquet(f"{model_path}/test_data_{timestamp}.parquet")
    print("Model and datasets saved successfully.")


spark = SparkSession.builder \
    .master(f"local[*]") \
    .appName("LandingToFormatted") \
    .getOrCreate()

con, cur = connect_postgresql(host='localhost',
                              database='bdm',
                              user='postgres',
                              password='postgres')

df = build_dataset(spark)

# impute floor column

categorical_columns = ['district', 'neighborhood', 'propertyType', 'status']

numerical_columns = ['bathrooms', 'distance', 'floor', 'priceByArea', 'rooms', 'size', 'year', 'month',
                     'incidents', 'population', 'RFD', 'exterior']

grown_truth = 'price'

(training_df, test_df) = df.randomSplit([0.7, 0.3])

median_values_train = training_df.groupBy('id_neighborhood').agg(round(median(col('floor'))))
df_train_final = impute_floor_varible(training_df, median_values_train)
df_test_final = impute_floor_varible(test_df, median_values_train)

models = {'regression_random_forest': RandomForestRegressor(featuresCol="features", labelCol=grown_truth),
          'linear_regression': LinearRegression(featuresCol='features', labelCol=grown_truth),
          'regression_decision_forest': DecisionTreeRegressor(featuresCol='features', labelCol=grown_truth),
          'regression_gbt': GBTRegressor(featuresCol='features', labelCol=grown_truth)
          }

df_train_final.cache()
df_test_final.cache()
for model_name, model in models.items():

    pipeline = build_pipeline(model)
    # Train model.  This also runs the indexer.
    model = pipeline.fit(df_train_final)
    model_type = model_name
    # Make predictions.
    predictions = model.transform(df_test_final)
    save_model_and_datasets(model, df_train_final, df_test_final, MODEL_HDFS)
    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol=grown_truth, predictionCol="prediction")

    # Calculate and print the evaluation metrics
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    print("R2 Score:", r2)
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    print("Root Mean Squared Error (RMSE):", rmse)
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    print("Mean Absolute Error (MAE):", mae)

    final_table = predictions.select("neighborhood", "latitude", "longitude", "prediction", grown_truth) \
        .withColumn('neighborhood', regexp_replace("neighborhood", "'", "")) \
        .groupBy('neighborhood') \
        .agg(
        mean("latitude").alias('latitude'),
        mean("longitude").alias('longitude'),
        mean(grown_truth).alias(grown_truth + "_mean"),
        mean('prediction').alias('prediction' + "_mean"),
        median(grown_truth).alias(grown_truth + "_median"),
        median('prediction').alias('prediction' + "_median"),
        count('prediction').alias('count_per_neighborhood')
    )

    final_table_columns = final_table.columns

    for values in final_table.collect():
        insert_values_to_table(table_name='results_neighborhood',
                               columns=final_table_columns + ['model_type', ],
                               values=tuple(values) + (model_type,),
                               conn=con,
                               cursor=cur)

    insert_values_to_table(table_name='model_metrics', columns=['model_type', 'r2', 'rmse', 'mae'],
                           values=(model_type, r2, rmse, mae), conn=con, cursor=cur)

close_connection(con, cur)
spark.stop()
