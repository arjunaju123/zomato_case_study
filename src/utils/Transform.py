import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from utils.common_utils import read_config

config = read_config('..\config.yml')

def fetch_staging_data(engine, staging_table_name):
    '''Fetch data from staging table'''
    with engine.connect() as conn:
        query = f"SELECT * FROM {staging_table_name}"
        staging_df = pd.read_sql(query, conn)
    return staging_df

def drop_columns(data, cols_to_drop):
    '''Drop specified columns from DataFrame'''
    data.drop(columns=cols_to_drop, inplace=True)

def remove_columns_with_high_null_percentage(data):
    '''Remove columns with missing values greater than null percentage(60 given in config file)%'''
    feature_na = [i for i in data.columns if data[i].isnull().sum() > 0]
    for i in feature_na:
        null_percentage = np.round((data[i].isnull().sum() / len(data[i])) * 100, 4)
        if null_percentage > config['params']['null_percentage_limit']:
            data.drop(columns=[i], inplace=True)
            print("{0} column dropped due to {1} percentage missing value".format(i,null_percentage))

def translate_ratings(data):
    '''Translate ratings from different languages to English'''
    # Define the mapping dictionary for translations
    translation_map = config['params']['translation_map']
    data['RATING_TYPE'] = data['RATING_TYPE'].map(translation_map)

def drop_duplicates(data):
    '''Drop duplicate rows'''
    data.drop_duplicates(inplace=True)

def create_location_dim_table(engine, data):
    '''Create location dimension table'''
    sql_table_name = 'location_dim_table'
    initial_sql = "CREATE TABLE IF NOT EXISTS " + sql_table_name + "(location_id INT AUTO_INCREMENT PRIMARY KEY"

    columns = config['params']['dimension_table_2_columns']
    create_sql(engine, data, columns,sql_table_name,sql=initial_sql)

def create_restaurant_dim_table(engine, data):
    '''Create restaurant dimension table'''
    sql_table_name = 'restaurant_dim_table'
    initial_sql = "CREATE TABLE IF NOT EXISTS " + sql_table_name + "(restaurant_id INT AUTO_INCREMENT PRIMARY KEY"

    columns = config['params']['dimension_table_1_columns']
    create_sql(engine, data, columns, sql_table_name,sql=initial_sql)

def create_fact_table(engine, data):
    '''Create fact table'''
    sql_table_name = 'fact_table'
    initial_sql = "CREATE TABLE IF NOT EXISTS " + sql_table_name + "(fact_id INT AUTO_INCREMENT PRIMARY KEY"
    columns = config['params']['fact_table_1_columns']
    create_sql(engine, data, columns, sql_table_name,sql=initial_sql)

def create_sql(engine, df, columns,sql_table_name,sql=''):
    '''Create MySQL schema'''
    def rename_df_cols(df):
        col_no_space = dict((i, i.replace(' ', '')) for i in list(df.columns))
        df.rename(columns=col_no_space, index=str, inplace=True)
        return df

    def dtype_mapping():
        return config['params']['dtype_mapping']

    df = rename_df_cols(df)
    map_data = dtype_mapping()

    print("sql table name is ",sql_table_name)

    if(sql_table_name=='fact_table'):

        for col in columns:
            if col in df.columns:
                dtype = str(df[col].dtype)
                mysql_dtype = map_data.get(dtype)
                if mysql_dtype:
                    sql += ", " + col + ' ' + mysql_dtype
                

        # Create SQL statement
        sql += ", restaurant_id INT, location_id INT"  # Add restaurant_id and location_id columns

        sql += ", FOREIGN KEY (restaurant_id) REFERENCES restaurant_dim_table(restaurant_id)"
        sql += ", FOREIGN KEY (location_id) REFERENCES location_dim_table(location_id)"

        sql += ")"

    elif(sql_table_name=='restaurant_dim_table' or sql_table_name=='location_dim_table'):

        # Create SQL statement
        for col in columns:
            dtype = str(df[col].dtype)
            mysql_dtype = map_data.get(dtype)
            if mysql_dtype:
                sql += ", " + col + ' ' + mysql_dtype

        sql += ")"

    print('\n', sql, '\n')
    
    try:
        conn = engine.raw_connection()
    except ValueError:
        print('You have a connection problem with MySQL, check engine parameters')

    cur = conn.cursor()

    try:
        cur.execute(sql)
    except ValueError:
        print("Schema creation failed, check SQL statement")

    cur.close()

def plot_pie_chart(data):
    '''Plot pie chart for top 3 cities'''
    city_names = data.CITY.value_counts().index
    city_val = data.CITY.value_counts().values

    plt.pie(city_val[:3], labels=city_names[:3], autopct='%1.2f%%')
    plt.show()

def plot_ratings_distribution(data):
    '''Plot ratings distribution'''
    plt.hist(data['RATING'], bins=5)
    plt.xticks(rotation=90)
    plt.title("Ratings Distribution")
    plt.show()

def plot_top_restaurants(data):
    '''Plot top 10 restaurants'''
    restaurant_counts = data['CITY'].value_counts()
    top_10_restaurants = restaurant_counts.head(10)

    plt.figure(figsize=(16, 10))
    sns.barplot(x=top_10_restaurants.index, y=top_10_restaurants.values)
    plt.xticks(rotation=90)
    plt.xlabel('City')
    plt.ylabel('Number of Restaurants')
    plt.title('Top 10 Restaurants in Various Locations')
    plt.show()

def plot_boxplot(data):
    '''Plot boxplot for restaurant ratings by cuisines'''
    plt.figure(figsize=(15, 8))
    sns.boxplot(x='CUSINETYPE', y='RATING', data=data)
    plt.xticks(rotation=90)
    plt.show()

def plot_scatter(data):
    '''Plot scatter plot for cost of dining vs hotel rating'''
    plt.figure(figsize=(8, 6))
    plt.scatter(data['PRICE'], data['RATING'], alpha=0.5)
    plt.title('Scatter Plot of Cost of Dining vs. Hotel Rating')
    plt.xlabel('Cost of Dining')
    plt.ylabel('Hotel Rating')
    plt.grid(True)
    plt.show()

# Main function
# if __name__ == "__main__":
#     engine = connect_to_database()
#     staging_table_name = 'staging_data'
#     data = fetch_staging_data(engine, staging_table_name)

#     cols_to_drop = ['PAGENO', 'URL']
#     drop_columns(data, cols_to_drop)
#     remove_columns_with_high_null_percentage(data)
#     translate_ratings(data)
#     drop_duplicates(data)

#     create_location_dim_table(engine, data)
#     create_restaurant_dim_table(engine, data)
#     create_fact_table(engine, data)

#     plot_pie_chart(data)
#     plot_ratings_distribution(data)
#     plot_top_restaurants(data)
#     plot_boxplot(data)
#     plot_scatter(data)
