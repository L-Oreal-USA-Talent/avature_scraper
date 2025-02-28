import sqlite3
from pathlib import Path


class DataBase:
    """
    Used to connect, write to and read from a local sqlite3 database
    """

    def __init__(self, db_file: Path):
        """Create a database in the file. Access the connection and cursor attributes.

        :param db_file: Database that contains database.
        """
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_file)
        except FileNotFoundError:
            print("f{db_file} does not exist.")

        self.cursor = self.conn.cursor()
        self._create_tables()

    def close(self):
        """
        close the db connection
        :return: None
        """
        self.conn.close()

    def _create_tables(self):
        """
        create new database table if one doesn't exist
        :return: None
        """
        offer_query = """
            CREATE TABLE IF NOT EXISTS offers (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        job_query = """
            CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        funnel_query = """
            CREATE TABLE IF NOT EXISTS funnel (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """
        applicant_query = """
            CREATE TABLE IF NOT EXISTS applicants (
            id INTEGER PRIMARY KEY AUTOINCREMENT
        ) """

        self.cursor.execute(offer_query)
        self.conn.commit()

        self.cursor.execute(job_query)
        self.conn.commit()

        self.cursor.execute(funnel_query)
        self.conn.commit()

        self.cursor.execute(applicant_query)
        self.conn.commit()

    def get_all_data(self, tbl: str, name: str = None) -> list[str]:
        """Get rows of data from a table.

        :param tbl: Name of the table in the database.
        :param name: Full name of recruiter. Defaults to None.
        :return: list[str]
        """
        if not name:
            query = f"SELECT * FROM {tbl}"
            self.cursor.execute(query)
        else:
            query = f"SELECT * FROM {tbl} WHERE Recruiter = ?"
            self.cursor.execute(query, (name,))

        result = self.cursor.fetchall()

        return result

    def get_jobs_by_recruiter(self, name=None):
        """Get rows of job data filtered by recruiter.

        :param name: _description_, defaults to None
        :return: _description_
        """
        return self.get_all_data(name, tbl="jobs")

    def get_offers_by_recruiter(self, name=None):
        """Get rows of offers filtered by recruiter.

        :param name: _description_
        :return: _description_
        """
        return self.get_all_data(name, tbl="offers")
