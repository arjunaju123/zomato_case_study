# main.py
import sqlalchemy
import pymysql
from ML_utils.data_loader import load_data
from ML_utils.data_preprocessing import preprocess_data
from utils.common_utils import connect_to_database,read_config
import pandas as pd

# Connect to MySQL database
ML_config = read_config('..\ML_config.yml')
engine = connect_to_database(ML_config)

staging_table_names = [ML_config['params']['fact_table_1'], ML_config['params']['dimension_table_1'], ML_config['params']['dimension_table_2']]

print("staging started...")
# Load data from database
staging_tables = load_data(engine, staging_table_names)
print("staging completed...")

print(staging_tables)

# Preprocess data
print("prprocessing started...")
X_train, X_test, y_train, y_test = preprocess_data(staging_tables, ML_config['params']['Target_column'])
print("prprocessing completed...")

print(X_train.shape)
print(y_train.shape)
print(X_test.shape)
print(y_test.shape)

##### Only import the ML_utils module below. #####

from ML_utils.modeling import train_linear_regression
from ML_utils.modeling import train_decision_tree
from ML_utils.modeling import train_random_forest
from ML_utils.modeling import evaluate_model

# Train models
lr_model = train_linear_regression(X_train, y_train)
dt_model = train_decision_tree(X_train, y_train)
rf_model = train_random_forest(X_train, y_train)

# Evaluate models
lr_mae, lr_rmse = evaluate_model(lr_model, X_test, y_test)
dt_mae, dt_rmse = evaluate_model(dt_model, X_test, y_test)
rf_mae, rf_rmse = evaluate_model(rf_model, X_test, y_test)

# Print evaluation metrics
print("Linear Regression - MAE:", lr_mae, "RMSE:", lr_rmse)
print("Decision Tree - MAE:", dt_mae, "RMSE:", dt_rmse)
print("Random Forest - MAE:", rf_mae, "RMSE:", rf_rmse)
