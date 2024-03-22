def add_auto_increment_ids(data, start_id=1):
    '''Add auto-increment IDs to DataFrame'''
    # If restaurant_id and location_id columns don't exist, create them
    if 'restaurant_id' not in data.columns:
        data['restaurant_id'] = range(start_id, start_id + len(data))
    if 'location_id' not in data.columns:
        data['location_id'] = range(start_id, start_id + len(data))
    return data

def process_data(data, start_id=1):
    '''Process data and add auto-increment IDs'''
    # If restaurant_id and location_id columns exist, append values to existing data
    if 'restaurant_id' in data.columns and 'location_id' in data.columns:
        max_restaurant_id = data['restaurant_id'].max()
        max_location_id = data['location_id'].max()
        new_data = add_auto_increment_ids(data, max(max_restaurant_id, max_location_id) + 1)
        #print("shape of data after loading in new_data is:",new_data.shape)
        return new_data
    # If restaurant_id and location_id columns don't exist, create them and add values
    else:
        return add_auto_increment_ids(data, start_id)


def load_data_into_table(data, table_name, engine, columns=None):
    '''Load data into specified table'''
    #print("columns in data before processing:", data.columns)
    processed_data = process_data(data)
    #print("Processed data shape is:", processed_data.shape)
    if columns is None:
        columns = processed_data.columns
    #print(columns)
    #print(processed_data.head(3))    
    # Load data into the table
    processed_data[columns].to_sql(table_name, con=engine, if_exists='append', index=False)

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
