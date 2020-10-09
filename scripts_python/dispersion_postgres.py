import pandas as pd
import psycopg2
import sqlalchemy
import numpy as np

def createTables():

    conn = psycopg2.connect(host="postgres", database="conekta",port = 5432, user="airflow", password="airflow")
    conn.autocommit = True
    cursor = conn.cursor()
    sqlCreateCompanies = """ 
    create table companies
    (
        company_id varchar(100) not null
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


def convertNone(string):
    string = str(string)
    if (string.strip() == "nan") or (string == "None"):
         return "-"
    else:
        return string


def changeCompanyName(string):
    string = str(string)
    if (string == "MiPas0xFFFF") or (string == "MiP0xFFFF") or ( string.startswith("MiP") ):
         return "MiPasajefy"
    else:
        return string


def applyEtlParquetToPostgres():
    
    conn = psycopg2.connect(host="postgres", port = 5432, user="airflow", password="airflow")
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

    
    df = pd.read_parquet("/usr/local/airflow/dags/src/data/data_formatted.parquet")

    engine = sqlalchemy.create_engine("postgresql://airflow:airflow@postgres/conekta")
    con = engine.connect()

    df.rename(columns={"name": "company_name", "paid_at": "updated_at"}, inplace=True)

    df_companies = df[["company_name","company_id"]].drop_duplicates()

    df_companies["company_id"] = df_companies["company_id"].map(lambda x: convertNone(x))
    df_companies["company_name"] = df_companies["company_name"].map(lambda x: convertNone(x))
    df_companies["company_name"] = df_companies["company_name"].map(lambda x: changeCompanyName(x))
    
    df_transaccions = df[["id","amount","status", "created_at","updated_at","company_id"]]

    df_companies = df_companies[~ (df_companies["company_id"] == "*******")]

    df_companies = df_companies[((df_companies.company_name!="-") & (df_companies.company_id!="-"))].drop_duplicates()

    table_companies = 'companies'
    table_tran = 'transacciones'

    dfCompanies = pd.read_sql("SELECT t.company_id FROM public.companies t", con).rename(columns={"company_id": "companyid"})


    df_result = pd.merge(df_companies, dfCompanies , left_on='company_id', right_on='companyid', how="left")

    df_companies = df_result[df_result.companyid.isnull()][["company_id","company_name"]]
    
    df_companies.to_sql(table_companies, con,if_exists='append', index=False)
    df_transaccions.to_sql(table_tran, con,if_exists='replace', index=False)

    con.close()