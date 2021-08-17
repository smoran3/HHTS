import psycopg2
import pandas as pd
from sqlalchemy import create_engine

#change for your setup
DB_NAME = "HHTS"

#database connection
db_connection_info = {
    "database": DB_NAME,
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "root"
}
connection = psycopg2.connect(**db_connection_info)
cur = connection.cursor() 

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

listtables = []
for i in range(0, len(GetTableList('public'))):
    listtables.append(GetTableList('public')[i][0])

conn_string = 'postgres://postgres:root@localhost/HHTS'

def df_clean(df):
    # Replace "Column Name" with "column_name"
    df.columns = df.columns.str.replace(" ", "_")
    df.columns = [x.lower() for x in df.columns]
    # Remove '.' and '-' from column names.       
    for s in [".", "-", "(", ")", "+"]:
        df.columns = df.columns.str.replace(s, "", regex=False)
    return df
    

#Read and Load Household table
if "household" not in listtables:
    hh_xlsx = "C:/Users/smoran/Downloads/PublicDB_RELEASE/DVRPC HTS Database Files/1_Household_Public.xlsx"
    df = pd.read_excel(hh_xlsx)
    #clean db
    df_clean(df)
    #write to database
    engine = create_engine(conn_string)
    df.to_sql('household', engine, schema = 'public')
else:
    print("already in database")

#Load Person table
if "person" not in listtables:
    per_xlsx = "C:/Users/smoran/Downloads/PublicDB_RELEASE/DVRPC HTS Database Files/2_Person_Public.xlsx"
    df = pd.read_excel(per_xlsx)
    #write to database
    engine = create_engine(conn_string)
    df.to_sql('person', engine, schema = 'public')
else:
    print("already in database")

#Load Vehicle table
if "vehicle" not in listtables:
    veh_xlsx = "C:/Users/smoran/Downloads/PublicDB_RELEASE/DVRPC HTS Database Files/3_Vehicle_Public.xlsx"
    df = pd.read_excel(veh_xlsx)
    #write to database
    engine = create_engine(conn_string)
    df.to_sql('vehicle', engine, schema = 'public')
else:
    print("already in database")

#Load Trip table
if "trip" not in listtables:
    trip_xlsx = "C:/Users/smoran/Downloads/PublicDB_RELEASE/DVRPC HTS Database Files/4_Trip_Public.xlsx"
    df = pd.read_excel(trip_xlsx)
    #write to database
    engine = create_engine(conn_string)
    df.to_sql('trip', engine, schema = 'public')
else:
    print("already in database")
