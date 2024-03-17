import yaml
import pandas as pd
import sqlalchemy
from utils.extract import fetch_lat_long_parallel
from utils.staging import create_sql
from utils.Transform import fetch_staging_data,drop_columns,remove_columns_with_high_null_percentage,translate_ratings,drop_duplicates
from utils.Transform import create_location_dim_table,create_restaurant_dim_table,create_fact_table
from utils.Transform import plot_pie_chart,plot_ratings_distribution,plot_top_restaurants,plot_boxplot,plot_scatter
from utils.Load import load_data_into_table
import logging
from utils.common_utils import read_config,connect_to_database

# Extract function
def extract_data(input_file_path,output_file_path):
    logging.info("Extracting data...")
    # Read your dataset into a DataFrame
    fetch_lat_long_parallel(input_file_path, output_file_path)
    df = pd.read_csv(output_file_path)
    logging.info("Extracting data completed.")
    return df

# Staging function
def stage_data(df,engine):
    logging.info("Staging data...")
    # Define the table name for the staging data
    staging_table_name = 'staging_data'

    initial_sql = "CREATE TABLE IF NOT EXISTS " +str(staging_table_name)+ "(key_pk INT AUTO_INCREMENT PRIMARY KEY"

    create_sql(engine, df= df,sql = initial_sql)

    # Load data into the staging table
    df.to_sql(staging_table_name, con=engine, if_exists='replace', index=False)
    logging.info("Staging data completed.")

# Transformation function
def transform_data(engine):
    # Load data from the staging table
    logging.info("Transforming data...")
    staging_table_name = 'staging_data'
    staging_df = fetch_staging_data(engine, staging_table_name)
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
def load_data(data,engine,config):
    logging.info("Loading data...")
    # Load data into the fact and dimension tables
    # Example: Load data into restaurant_dim_table, location_dim_table, fact_table, etc.

    # Load data into the restaurant dimension table
    restaurant_dim_table_name = config['params'] ['dimension_table_1'] # Update with your restaurant dimension table name
    restaurant_columns = config['params'] ['dimension_table_1_columns']
    load_data_into_table(data, restaurant_dim_table_name, engine, columns=restaurant_columns)
    print("columns in data during loading are:",data.columns)

    # Load data into the location dimension table
    location_dim_table_name = config['params'] ['dimension_table_2']  # Update with your location dimension table name
    location_columns = config['params'] ['dimension_table_2_columns']
    load_data_into_table(data, location_dim_table_name, engine, columns=location_columns)

    # Load data into the fact table
    fact_table_name = config['params'] ['fact_table_1']   # Update with your fact table name
    fact_columns = config['params'] ['fact_table_1_columns']
    load_data_into_table(data, fact_table_name, engine, columns=fact_columns)
    logging.info("Loading data completed.")