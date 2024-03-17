import pandas as pd
import logging
from utils.common_utils import read_config,connect_to_database
from utils.etl import extract_data,stage_data,transform_data,load_data

# Configure logging settings
logging.basicConfig(filename='..\logs\etl_pipeline.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():

    try:
        # Extract data
        config = read_config('..\config.yml')
        engine = connect_to_database(config)
        print("Extracting data...")
        #df = extract_data(config['files']['input_csv'],config['files']['output_csv'])
        df = pd.read_csv(r"..\data\output\restaurant_data_with_lat_long_46000_updated_ETL_Pipeline.csv")#For demo purposes
        print("Extracting data completed...")
        print("columns in data after extracting are:",df.columns)
        # Stage data
        print("Staging data...")
        stage_data(df,engine)
        print("Staging data completed...")
        # Transform data
        print("Transforming data...")
        transformed_data = transform_data(engine)
        print("Transforming data completed...")
        print("columns in data after transforming are:",transformed_data.columns)
        # Load data
        print("Loading data...")
        load_data(transformed_data,engine,config)
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
