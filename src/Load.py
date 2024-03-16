def load_data_into_table(data, table_name, engine, columns=None):
    '''Load data into specified table'''
    if columns is None:
        columns = data.columns
    data[columns].to_sql(table_name, con=engine, if_exists='append', index=False)

# # Load data into the restaurant dimension table
# restaurant_dim_table_name = 'restaurant_dim_table'  # Update with your restaurant dimension table name
# restaurant_columns = ['NAME', 'CUSINE_CATEGORY', 'CUSINETYPE', 'TIMING', 'RATING_TYPE']
# load_data_into_table(data, restaurant_dim_table_name, engine, columns=restaurant_columns)

# # Load data into the location dimension table
# location_dim_table_name = 'location_dim_table'  # Update with your location dimension table name
# location_columns = ['REGION', 'Latitude', 'Longitude', 'CITY']
# load_data_into_table(data, location_dim_table_name, engine, columns=location_columns)

# # Load data into the fact table
# fact_table_name = 'fact_table'  # Update with your fact table name
# fact_columns = ['RATING', 'VOTES', 'PRICE']
# load_data_into_table(data, fact_table_name, engine, columns=fact_columns)
