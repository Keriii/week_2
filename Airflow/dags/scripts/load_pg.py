import pandas as pd
from sqlalchemy import create_engine
import psycopg2


def create_engines():
    """
    creates engine using sqlalchemy for a given paramters:
    """
    engine = create_engine("postgresql://postgres:kerod53@localhost:5432/Sensor_data")
    return engine

def write_dataframe_to_table(df: pd.DataFrame, table_name: str):
    """
    Writes a pandas dataframe to a new table in the PostgreSQL database.
    """
    engine = create_engine("postgresql://postgres:kerod53@localhost:5432/Sensor_data")

    df.to_sql(table_name, engine, index=False, if_exists='replace')
    print(f"Dataframe successfully written to the '{table_name}' table.")

@transform_df
def load_pg():

    #now lets load them into our database
    write_dataframe_to_table(vehicle_data, 'vehicle_data')
    write_dataframe_to_table(trajectory_data, 'trajectory_data')