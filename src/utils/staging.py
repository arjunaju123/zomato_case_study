import pandas as pd
import sqlalchemy
from utils.common_utils import read_config

config = read_config('..\config.yml')

def rename_df_cols(df):
    '''Input a dataframe, outputs same dataframe with No Space in column names'''
    col_no_space =  dict((i, i.replace(' ','')) for i in list(df.columns))
    df.rename(columns= col_no_space, index= str, inplace= True)
    return df

def dtype_mapping():
    '''Returns a dict to refer correct data type for mysql'''
    return config['params']['dtype_mapping']

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