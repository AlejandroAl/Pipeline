from sqlalchemy import create_engine
import pymysql
import pandas as pd



def  loadData():

    try:
        mysqlPass = os.getenv('MYSQPASS')
        mysqlUser = os.environ.get('MYSQLUSER')
    except:
        mysqlUser = "alex"
        mysqlPass = "p4ssw0rd"

    connectionString= f"mysql+pymysql://{mysqlUser}:{mysqlPass}@mysql/conekta"
        
    print(connectionString)
    sqlEngine       = create_engine(connectionString, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()

    data_url_file = "/usr/local/airflow/dags/src/data/data_prueba_tecnica.csv"

    df = pd.read_csv(data_url_file,index_col=False)

    print(df)

    df.to_sql("raw_data", dbConnection, if_exists='replace', index=False)

    dbConnection.close()