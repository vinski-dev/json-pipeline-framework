import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def get_db_connection():
    # In production, pull these from Airflow Connections or Environment Variables
    return psycopg2.connect(
        host="host.docker.internal", database="analytics_lab", user="postgres", password="14myNmax2021", port="5432"
    )


def handle_schema_drift(cursor, table_name, df):
    """Detects missing columns and dynamically alters the table (Schema-on-Read)."""
    # Check if table exists
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
    """)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        # Auto-create baseline table (simplifying types as text for bronze layer ingestion)
        columns = [f"{col} TEXT" for col in df.columns]
        create_sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"
        cursor.execute(create_sql)
        # Add the unique constraint for our idempotency hash
        cursor.execute(f"ALTER TABLE {table_name} ADD CONSTRAINT unique_hash UNIQUE (_pipeline_record_hash);")
        print(f"Created new table: {table_name}")
        return

    # Check for additive drift
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}';")
    existing_columns = [row[0] for row in cursor.fetchall()]

    new_columns = [col for col in df.columns if col not in existing_columns]

    for col in new_columns:
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN {col} TEXT;"
        cursor.execute(alter_sql)
        print(f"Schema Drift Detected: Added new column '{col}'")


def insert_idempotent_bronze(table_name, df):
    """Inserts records immutably, ignoring duplicates based on the deterministic hash."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # 1. Enforce Schema Match
        handle_schema_drift(cursor, table_name, df)

        # 2. Prepare Data for Insertion
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]

        # 3. The Idempotent Execution (PostgreSQL specific 'ON CONFLICT DO NOTHING')
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES %s 
            ON CONFLICT (_pipeline_record_hash) DO NOTHING;
        """

        execute_values(cursor, insert_query, values)

        conn.commit()
        print(f"Successfully processed {len(df)} records into {table_name}.")

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


class conn:
    """Thin, useful wrapper around a PostgreSQL connection for the pipeline."""

    def __init__(self, host="host.docker.internal", database="analytics_lab", user="postgres",
                 password="14myNmax2021", port="5432", **kwargs):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection_kwargs = {"host": host, "database": database, "user": user,
                                 "password": password, "port": port, **kwargs}
        self._connection = psycopg2.connect(**self.connection_kwargs)
        self._cursor = self._connection.cursor()

    def execute(self, query, params=None):
        """Execute a SQL query and return the cursor for chaining."""
        if params is None:
            self._cursor.execute(query)
        else:
            self._cursor.execute(query, params)
        return self._cursor

    def fetchone(self, query, params=None):
        """Execute a query and return the first row."""
        self.execute(query, params)
        return self._cursor.fetchone()

    def fetchall(self, query, params=None):
        """Execute a query and return all rows."""
        self.execute(query, params)
        return self._cursor.fetchall()

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def close(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def insert_dataframe(self, table_name, df):
        """Convenience method for the bronze ingest pattern used in this project."""
        handle_schema_drift(self._cursor, table_name, df)
        columns = list(df.columns)
        values = [tuple(x) for x in df.to_numpy()]
        insert_query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT (_pipeline_record_hash) DO NOTHING;
        """
        execute_values(self._cursor, insert_query, values)
        self._connection.commit()
        return len(df)

    @property
    def cursor(self):
        return self._cursor

    @property
    def connection(self):
        return self._connection
