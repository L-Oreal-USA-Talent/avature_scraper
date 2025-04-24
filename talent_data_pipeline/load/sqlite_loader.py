import sqlite3
from pathlib import Path
import pandas as pd
from pandas import DataFrame


class DataBase:
    """
    Used to connect, write to and read from a local sqlite3 database.
    """

    def __init__(self, db_file: Path):
        """Create a connection to the database.

        Args:
            db_file (Path): Path to a .db file.
        """
        try:
            self.conn = sqlite3.connect(db_file)
        except FileNotFoundError:
            print("f{db_file} does not exist.")

        self.cursor = self.conn.cursor()
        self._create_tables()

    def close(self):
        """Close database."""
        self.conn.close()

    def _create_tables(self):
        """Create initial tables in the database."""
        job_query = """
            CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        self.cursor.execute(job_query)

        applicant_query = """
            CREATE TABLE IF NOT EXISTS applicants (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        self.cursor.execute(applicant_query)

        funnel_query = """
            CREATE TABLE IF NOT EXISTS funnel (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        self.cursor.execute(funnel_query)

        event_funnel_query = """
            CREATE TABLE IF NOT EXISTS event_funnel (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        )"""
        self.cursor.execute(event_funnel_query)

        self.conn.commit()

    def replace_data(self, table_name: str, df: DataFrame) -> None:
        """Replace all data in table_name with data from df.
        Args:
            table_name (str): Name of the table in the database.
            df (DataFrame): DataFrame that will refresh table_name.
        """

        # Delete existing data
        self.cursor.execute(f"DELETE FROM {table_name}")

        # Insert new data
        df.to_sql(table_name, self.conn, if_exists="append", index=False)

        self.conn.commit()

    def query_to_dataframe(self, query: str, col_dtypes: dict = None) -> DataFrame:
        """Execute SQL query and return results as a Pandas DataFrame.

        Args:
            query (str): Query that returns results.
            col_dtypes (dict): A dictionary of column mapping to data types.

        Returns:
            DataFrame: DataFrame containing executed query.
        """
        return pd.read_sql_query(query, self.conn, dtype=col_dtypes)

    def get_table_info(self, table_name: str) -> dict:
        """Get detailed information about the table.

        Args:
            table_name (str): Table in the database.

        Returns:
            dict: Dictionary with table name, row count, and column information.
        """

        # Get table schema
        self.cursor.execute(f"PRAGMA table_info({table_name})")
        schema = self.cursor.fetchall()

        # Get row count
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = self.cursor.fetchone()[0]

        self.conn.close()

        # Format results
        columns = []
        for col in schema:
            columns.append(
                {
                    "cid": col[0],
                    "name": col[1],
                    "type": col[2],
                    "notnull": col[3],
                    "default_value": col[4],
                    "pk": col[5],
                }
            )

        return {"table_name": table_name, "row_count": count, "columns": columns}
