# data_preprocessing.py

from sklearn.model_selection import train_test_split
import pandas as pd
from utils.common_utils import read_config

# Connect to MySQL database
ML_config = read_config('..\ML_config.yml')

def preprocess_data(df, target_column):
    # Drop rows with missing target values
    #print(df.columns)
    num_rows = len(df)
    #print("Number of rows:", num_rows)

    missing_values = df.isnull().sum()
    #print("Number of missing values in dataframe is:", missing_values)

    df = df.loc[:,~df.columns.duplicated()].copy()
    # Print the DataFrame after removing duplicate columns
    #print(df.columns)
    
    uselessColumns = ['fact_id','restaurant_id','location_id','NAME']

    df = df.drop(uselessColumns,axis=1)
    #print(df.columns)

    missing_values = df.isnull().sum()
    #print("Number of missing values in dataframe is:", missing_values)

    df['cusine_count'] = df['CUSINE_CATEGORY'].str.count(',') + 1
    #print("initial df cusine count is",df)

    df = df.dropna(subset=['Latitude','Longitude','RATING','cusine_count','RATING_TYPE'])
    #print("initial df filtered is",df)

    missing_values = df.isnull().sum()
    print("Number of missing values in dataframe after dropping null values is:", missing_values)
    #print("shape of data is ",df.shape)
    selected_columns = ML_config['params']['selected_columns_for_prediction']
    # filtered_df=filtered_df.dropna(subset=[target_column], inplace=True)
    # print("initial df filtered is",filtered_df)
    filtered_df = df[selected_columns]

    #print("1:df is",df)
    
    # Encode ordinal categorical column
    category_mapping = ML_config['params']['rating_type_category_mapping']
    # print(df.columns)
    filtered_df['RATING_TYPE'] = df['RATING_TYPE'].map(category_mapping)

    #print("2:df is",df)
    
    # Convert nominal categorical columns to numeric using one-hot encoding
    filtered_df = pd.get_dummies(filtered_df, columns=['CUSINETYPE', 'CITY'])

    #print("3:df is",filtered_df)
    print(filtered_df.shape)

    print("columns in df is",filtered_df.columns)
    
    # Split data into features and target variable
    X = filtered_df
    y = df[target_column]

    #print(' X is',X)
    #print('y is',y)
    #print('X shape is',X.shape)
    #print('y shape is',y.shape)
    
    # Split data into train and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=ML_config['params']['test_size'], random_state=ML_config['params']['random_state'])
    
    return X_train, X_test, y_train, y_test
