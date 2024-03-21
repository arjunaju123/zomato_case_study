# data_loader.py

import pandas as pd

#staging_table_names = ['fact_table', 'restaurant_dim_table', 'location_dim_table']

def load_data(engine, staging_table_names):
    # Create an empty list to store DataFrames
    print("engine is", engine)
    dfs = []
    # Connect to the database and retrieve the staging data for each table
    for table_name in staging_table_names:
        with engine.connect() as conn:
            # Query the staging data
            query = f"SELECT * FROM {table_name}"
            print(query)
            df = pd.read_sql(query, conn)
            #print("shape is", df.shape)
            dfs.append(df)
    # Concatenate the DataFrames in df_list into a single DataFrame
    combined_df = pd.concat(dfs, axis=1)
    #print("combined_df is", combined_df)
    return combined_df

