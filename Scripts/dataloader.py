import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def connection():
    """
    connects to my pg database
    """
    conn = psycopg2.connect(dbname = 'Sensor_data',
                            user = 'postgres',
                            password = 'kerod53@',
                            host = 'localhost',
                            port = '5432')
    return conn

def table_to_sql(conn, table_name:str) -> pd.DataFrame:
    query = f'SELECT * FROM public.{table_name}'
    data = pd.read_sql_query(query, conn)

    return data

def create_engines():
    """
    creates engine using sqlalchemy for a given paramters:
    """
    engine = create_engine("postgresql://postgres:kerod53@@localhost:5432/Sensor_data")
    return engine

def write_dataframe_to_table(df: pd.DataFrame, table_name: str)->None:
    """
    Writes a pandas dataframe to a new table in the PostgreSQL database.
    """
    engine = create_engine("postgresql://postgres:kerod53@@localhost:5432/Sensor_data")

    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"Dataframe successfully written to the '{table_name}' table.")