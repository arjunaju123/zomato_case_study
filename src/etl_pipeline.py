import pandas as pd
import sqlalchemy
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
from extract import fetch_lat_long_parallel
from staging import create_sql
from Transform import fetch_staging_data,drop_columns,remove_columns_with_high_null_percentage,translate_ratings,drop_duplicates
from Transform import create_location_dim_table,create_restaurant_dim_table,create_fact_table
from Transform import plot_pie_chart,plot_ratings_distribution,plot_top_restaurants,plot_boxplot,plot_scatter
from Load import load_data_into_table
import logging

# Configure logging settings
logging.basicConfig(filename='..\logs\etl_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define database connection details
db_username = 'root'
db_password = 'experion%40123'
db_host = 'localhost'
db_port = '3306'
db_name = 'zomato_db'

# Define the engine to connect to the MySQL database
engine = sqlalchemy.create_engine(f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Extract function
def extract_data():
    logging.info("Extracting data...")
    # Read your dataset into a DataFrame
    fetch_lat_long_parallel(r"..\data\output\restaurant_data_with_lat_long_46000.csv", r"..\data\output\restaurant_data_with_lat_long_46000_updated_ETL_Pipeline.csv")
    df = pd.read_csv(r"..\data\output\restaurant_data_with_lat_long_46000_updated_ETL_Pipeline.csv")
    logging.info("Extracting data completed.")
    return df

# Staging function
def stage_data(df):
    logging.info("Staging data...")
    # Define the table name for the staging data
    staging_table_name = 'staging_data'

    initial_sql = "CREATE TABLE IF NOT EXISTS " +str(staging_table_name)+ "(key_pk INT AUTO_INCREMENT PRIMARY KEY"

    create_sql(engine, df= df,sql = initial_sql)

    # Load data into the staging table
    df.to_sql(staging_table_name, con=engine, if_exists='replace', index=False)
    logging.info("Staging data completed.")

# Transformation function
def transform_data():
    # Load data from the staging table
    logging.info("Transforming data...")
    staging_table_name = 'staging_data'

    staging_df = fetch_staging_data(engine, staging_table_name)
    # with engine.connect() as conn:
    #     # Query the staging data
    #     query = f"SELECT * FROM staging_data"
    #     staging_df = pd.read_sql(query, conn)
    print("columns in data for transforming are:",staging_df.columns)

    data = staging_df.copy()

    print("copied columns in data for transforming are:",staging_df.columns)

    cols_to_drop = ['PAGENO', 'URL']
    drop_columns(data, cols_to_drop)
    remove_columns_with_high_null_percentage(data) #if columns has greater than 60% null remove column.May throw error sometimes because some important column for loading get removed
    translate_ratings(data)
    drop_duplicates(data)

    create_location_dim_table(engine, data)
    create_restaurant_dim_table(engine, data)
    create_fact_table(engine, data)

    # plot_pie_chart(data)
    # plot_ratings_distribution(data)
    # plot_top_restaurants(data)
    # plot_boxplot(data)
    # plot_scatter(data)

    logging.info("Transforming data completed.")
    return data

# Loading function
def load_data(data):
    logging.info("Loading data...")
    # Load data into the fact and dimension tables
    # Example: Load data into restaurant_dim_table, location_dim_table, fact_table, etc.

    # Load data into the restaurant dimension table
    restaurant_dim_table_name = 'restaurant_dim_table'  # Update with your restaurant dimension table name
    restaurant_columns = ['NAME', 'CUSINE_CATEGORY', 'CUSINETYPE', 'TIMING', 'RATING_TYPE']
    load_data_into_table(data, restaurant_dim_table_name, engine, columns=restaurant_columns)
    print("columns in data during loading are:",data.columns)

    # Load data into the location dimension table
    location_dim_table_name = 'location_dim_table'  # Update with your location dimension table name
    location_columns = ['REGION', 'Latitude', 'Longitude', 'CITY']
    load_data_into_table(data, location_dim_table_name, engine, columns=location_columns)

    # Load data into the fact table
    fact_table_name = 'fact_table'  # Update with your fact table name
    fact_columns = ['RATING', 'VOTES', 'PRICE']
    load_data_into_table(data, fact_table_name, engine, columns=fact_columns)
    logging.info("Loading data completed.")

def main():

    try:
        # Extract data
        print("Extracting data...")
        df = extract_data()
        #df = pd.read_csv(r"..\data\output\restaurant_data_with_lat_long_46000_updated_ETL_Pipeline.csv")#For demo purposes
        print("Extracting data completed...")
        print("columns in data after extracting are:",df.columns)
        # Stage data
        print("Staging data...")
        stage_data(df)
        print("Staging data completed...")
        # Transform data
        print("Transforming data...")
        transformed_data = transform_data()
        print("Transforming data completed...")
        print("columns in data after transforming are:",transformed_data.columns)
        # Load data
        print("Loading data...")
        load_data(transformed_data)
        logging.info("ETL Pipeline completed successfully.")
        print("Loading data completed...")
        print("ETL Pipeline completed successfully.")

        latitude_values_count = transformed_data['Latitude'].count()

        print("Total number of values in the 'latitude' column:", latitude_values_count)
        print("Total size of dataset:", transformed_data.shape)
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
