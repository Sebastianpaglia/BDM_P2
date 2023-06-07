import os
import pandas as pd
import psycopg2
import pyarrow


# Convert Parquet files to CSV
parquet_directory = "C:/Users/sebas/OneDrive/Escritorio/Subjects/BDM/Project2/idealista_incidents_formatted_zone/idealista_incidents_formatted_zone"
csv_directory = "C:/Users/sebas/OneDrive/Escritorio/Subjects/BDM/Project2/idealista_incidents_formatted_zone/csv_idealista_incidents_formatted_zone"
income_csv_directory = "C:/Users/sebas/OneDrive/Escritorio/Subjects/BDM/Project2/idealista_incidents_formatted_zone/csv_income_formatted_zone"
income_parquet_directory = "C:/Users/sebas/OneDrive/Escritorio/Subjects/BDM/Project2/idealista_incidents_formatted_zone/income_formatted_zone"

# Iterate over the Parquet files and convert them to CSV
for file in os.listdir(parquet_directory):
    if file.endswith(".parquet"):
        parquet_file_path = os.path.join(parquet_directory, file)
        csv_file_path = os.path.join(csv_directory, os.path.splitext(file)[0] + ".csv")
        df = pd.read_parquet(parquet_file_path, engine="pyarrow")
        df.to_csv(csv_file_path, index=False)

for file in os.listdir(income_parquet_directory):
    if file.endswith(".parquet"):
        parquet_file_path = os.path.join(income_parquet_directory, file)
        csv_file_path = os.path.join(income_csv_directory, os.path.splitext(file)[0] + ".csv")
        df = pd.read_parquet(parquet_file_path, engine="pyarrow")
        df.to_csv(csv_file_path, index=False)


# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='BDM',
    user='postgres',
    password='adsdb'
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

# Create the table !!!!!!!!!! FALTA ACLARAR TEMA PK Y FK !!!!!!!!!!!!!!!!
create_table_query = '''
    CREATE TABLE price_incidents ( \
        district CHAR(50), \
        id_neighborhood CHAR(50), \
        neighborhood CHAR(50), \
        city CHAR(50), \
        bathrooms INTEGER, \
        distance INTEGER, \
        exterior CHAR(50), \
        floor CHAR(50), \
        latitude NUMERIC(10, 1), \
        longitude NUMERIC(10, 1), \
        price NUMERIC(10, 1), \
        priceByArea NUMERIC(10, 1), \
        propertyType CHAR(50), \
        rooms INTEGER, \
        size NUMERIC(10, 1), \
        status CHAR(50), \
        year INTEGER, \
        month INTEGER,  \
        incidents INTEGER);
        
    CREATE TABLE income ( \
        neighborhood CHAR(50), \
        id_neighborhood CHAR(50), \
        district CHAR(50), \
        year INTEGER, \
        population INTEGER, \
        RFD NUMERIC(10,2));
    '''

# Execute the table creation query
cursor.execute(create_table_query)

# Commit the changes to the database
conn.commit()

# Load the CSV files into the table
for csv_file in os.listdir(csv_directory):
    if csv_file.endswith(".csv"):
        csv_file_path = os.path.join(csv_directory, csv_file)
        copy_query = f"COPY price_incidents FROM '{csv_file_path}' DELIMITER ',' CSV HEADER;"
        cursor.execute(copy_query)

for csv_file in os.listdir(income_csv_directory):
    if csv_file.endswith(".csv"):
        income_csv_file_path = os.path.join(income_csv_directory, csv_file)
        copy_query = f"COPY income FROM '{income_csv_file_path}' DELIMITER ',' CSV HEADER;"
        cursor.execute(copy_query)

# Commit the changes to the database
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()