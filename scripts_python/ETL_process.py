from sqlalchemy import create_engine
import pymysql
import pandas as pd
import datetime
import os


def applyETL_MysqlToParquet():

    try:
        mysqlUser = os.environ.get('MYSQLUSER')
        mysqlPass = os.environ.get('MYSQPASS')
    except:
        mysqlUser = "alex"
        mysqlPass = "p4ssw0rd"

    connectionString= f"mysql+pymysql://{mysqlUser}:{mysqlPass}@mysql/conekta"
        
    print(connectionString)
    sqlEngine       = create_engine(connectionString, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()

    df = pd.read_sql("select * from conekta.raw_data", dbConnection)

    convert_dict = {'id': str, 
                    'name': str,
                    'company_id': str,
                    'amount': float, 
                    'status':str
                    } 
    
    df = df.astype(convert_dict) 

    print(type(df["created_at"][0])) 

    def convertStringToTimeStamp(string):
        try:
            return_x = datetime.datetime.strptime(string,"%Y-%m-%d")
        except:
            return_x = datetime.datetime(1700,1,1)
        return return_x

    df["created_at"] = df["created_at"].apply(lambda x: convertStringToTimeStamp(x))
    df["paid_at"] = df["paid_at"].apply(lambda x: convertStringToTimeStamp(x))

    df.to_parquet("/usr/local/airflow/dags/src/data/data_formatted.parquet",index=False,compression='GZIP')

    dbConnection.close()