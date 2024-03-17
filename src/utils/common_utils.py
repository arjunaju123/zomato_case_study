import yaml
import logging
import sqlalchemy

def read_config(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logging.error(f"An error occurred while reading the config file: {e}")

def connect_to_database(config):
    try:
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{config['database']['username']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['name']}")
        return engine
    except Exception as e:
        logging.error(f"An error occurred while connecting to the database: {e}")