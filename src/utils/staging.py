import pandas as pd
import sqlalchemy
from utils.common_utils import read_config

config = read_config('..\config.yml')

# Read the CSV file into a DataFrame
#df = pd.read_csv(r'..\path_to_scraped_csv_file')

#engine = sqlalchemy.create_engine('mysql+pymysql://root:experion%40123@localhost:3306/zomato_db')

# sql_table_name= 'staging_data'
# initial_sql = "CREATE TABLE IF NOT EXISTS " +str(sql_table_name)+ "(key_pk INT AUTO_INCREMENT PRIMARY KEY"

def rename_df_cols(df):
    '''Input a dataframe, outputs same dataframe with No Space in column names'''
    col_no_space =  dict((i, i.replace(' ','')) for i in list(df.columns))
    df.rename(columns= col_no_space, index= str, inplace= True)
    return df

def dtype_mapping():
    '''Returns a dict to refer correct data type for mysql'''
    return config['params']['dtype_mapping']

#engine = sqlchemy.create_engine('mysql+pymsql://<username>:<password>@<server-name>:<port_number>/<database_name>')

def create_sql(engine, df, sql):
    '''input engine: engine (connection for mysql), df: dataframe that you would like to create a schema for,
        outputs Mysql schema creation'''

    df = rename_df_cols(df)

    col_list_dtype = [(i, str(df[i].dtype)) for i in list(df.columns)]

    map_data= dtype_mapping()

    for i in col_list_dtype:
        key = str(df[i[0]].dtypes)
        sql += ", " + str(i[0])+ ' '+ map_data[key]
    sql= sql + str(')')
    
    print('\n', sql, '\n')
    
    try:
        conn = engine.raw_connection()
    except ValueError:
        print('You have connection problem with Mysql, check engine parameters')
    
    cur = conn.cursor()
    
    try:
        cur.execute(sql)
    except ValueError: 
        print("Ohh Damn it couldn't create schema, check Sql again")
    
    cur.close()         
    
#create_sql(engine, df= df)

# Write DataFrame to MySQL table
#table_name = 'staging_data'
#df.to_sql(table_name, con=engine, if_exists='append', index=False) 