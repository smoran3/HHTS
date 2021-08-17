import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import os

####################################
# change for your setup
DB_NAME = "HHTS"
#database connection info
db_connection_info = {
    "database": DB_NAME,
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "root"
}
connection = psycopg2.connect(**db_connection_info)
cur = connection.cursor() 
#another format for connection info
conn_string = 'postgres://postgres:root@localhost/HHTS'
#where your HHTS excel files are located
filepath = "C:/Users/smoran/Downloads/PublicDB_RELEASE/DVRPC HTS Database Files/"
######################################

#get list of existing tables in DB
def GetTableList(t_schema):
    q_gettables = """
    SELECT
        table_name
    FROM information_schema.tables
    WHERE(
        table_schema = '%s'
        )
    ORDER BY table_name""" % t_schema
    #run query
    cur.execute(q_gettables)
    table_list = cur.fetchall()
    
    return(table_list)

#reformat that list
listtables = []
for i in range(0, len(GetTableList('public'))):
    listtables.append(GetTableList('public')[i][0])

#clean up column names for import
def df_clean(df):
    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]
    # Remove '.' and '-' from column names.       
    for s in [".", "-", "(", ")", "+"]:
        df.columns = df.columns.str.replace(s, "", regex=False)
    return df

#read excel file, clean, and write to database    
path = filepath + '%s'
def loadtables(tbl_name, excel_file_name):
    if tbl_name not in listtables:
        xlsx = path % excel_file_name
        df = pd.read_excel(xlsx)
        #clean dataframe
        df_clean(df)
        #write to db
        engine = create_engine(conn_string)
        df.to_sql(tbl_name, engine, schema = 'public')
    else:
        print("already in database")

#iterate over the files to run the above functions
db_tbl_names = ['household', 'person', 'vehicle', 'trip']
files = os.listdir(filepath)
for i in range(0,4):
    loadtables(db_tbl_names[i], files[i])