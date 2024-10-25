from sqlalchemy import create_engine
from pandas import DataFrame
from airflow.hooks.base_hook import BaseHook

class Load:
    def __init__(self, dataframe: DataFrame, postgres_conn_id: str) -> None:
        self.dataframe = dataframe
        self.postgres_conn_id = postgres_conn_id
        self.engine = self.create_engine()

    def create_engine(self):
        # Retrieve connection details from Airflow
        conn = BaseHook.get_connection(self.postgres_conn_id)
        connection_string = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"

        # Create the SQLAlchemy engine
        return create_engine(connection_string)

    def load(self) -> None:
        # Define the table name
        table_name = "covid_data"

        # Use a connection context manager to ensure the connection is properly managed
        with self.engine.connect() as connection:
            # Load the DataFrame to PostgreSQL using `to_sql`
            self.dataframe.to_sql(
                table_name,
                connection,  # Pass the connection instead of the engine
                if_exists="append",  # Append data
                index=False  # Don't write DataFrame index as a column
            )        # Use a connection context manager to ensure the connection is properly managed

        print(f"Loaded {len(self.dataframe)} rows to {table_name}")