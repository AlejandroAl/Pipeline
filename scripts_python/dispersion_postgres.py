import pandas as pd
import psycopg2
import sqlalchemy

def createTables():

    conn = psycopg2.connect(host="localhost", database="conekta",port = 5432, user="airflow", password="airflow")
    conn.autocommit = True
    cursor = conn.cursor()
    sqlCreateCompanies = """ 
    create table companies
    (
        company_id varchar(24) not null
            constraint companies_pk
			    primary key,
        company_name varchar(130) not null
    );
    """
    

    sqlCreateTransactions = """
    create table transacciones
    (
        id varchar(24) not null
            constraint transacciones_pk
                primary key,
        amount decimal(16,2) not null,
        status varchar(30) not null,
        created_at timestamp not null,
        updated_at timestamp,
        company_id varchar(24) not null
            constraint transacciones_companies_company_id_fk
                references companies (company_id)
    );
    """

    sqlCompanies = "SELECT * FROM pg_catalog.pg_tables  WHERE schemaname = 'public' AND tablename  = 'companies';"
    
    cursor.execute(sqlCompanies)

    value = cursor.fetchone()
    if not value:
        cursor.execute(sqlCreateCompanies)

    sqltransacciones = "SELECT * FROM pg_catalog.pg_tables  WHERE schemaname = 'public' AND tablename  = 'transacciones';"
    
    cursor.execute(sqltransacciones)

    value = cursor.fetchone()
    if not value:
        cursor.execute(sqlCreateTransactions)


    cursor.close()
    conn.close()



def applyEtlParquetToPostgres():
    
    conn = psycopg2.connect(host="localhost", port = 5432, user="airflow", password="airflow")
    conn.autocommit = True
    cursor = conn.cursor()
    sql = " SELECT 1 AS result FROM pg_database WHERE datname='conekta' "
    cursor.execute(sql)
    value = cursor.fetchone()
    if not value:
        sql = '''CREATE database conekta'''
        cursor.execute(sql)
    cursor.close()
    conn.close()

    createTables()

    
    df = pd.read_parquet("/home/alejandro/Documentos/projects/Conekta/Pipeline/mnt/airflow/dags/src/data/data_formatted.parquet")

    engine = sqlalchemy.create_engine("postgresql://airflow:airflow@localhost/conekta")
    con = engine.connect()

    print(df)

    table_name = 'testing'
    df.to_sql(table_name, con,if_exists='replace', index=False)

    print(engine.table_names())


    con.close()

applyEtlParquetToPostgres()



