import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Creates and returns a SQL Server connection.
    """

    conn = pyodbc.connect(
        f"""
        DRIVER={{ODBC Driver 18 for SQL Server}};
        SERVER={os.getenv('DB_SERVER')};
        DATABASE={os.getenv('DB_NAME')};
        Trusted_Connection=yes;
        TrustServerCertificate=yes;
        """,
        autocommit=False
    )

    return conn
