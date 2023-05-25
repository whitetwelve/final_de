import psycopg2
import snowflake.connector
import csv
import os
from connparams.ConnectionParams import PG_CONN_PARAMS
from connparams.ConnectionParams import SF_CONN_PARAMS

# Created By: Dea Chintia Putri
class PgToSfDataIngester:
    # Mapping of PostgreSQL data types to Snowflake data types
    DATA_TYPE_MAPPING = {
        "character varying": lambda length: f"VARCHAR({length})",
        "date": "DATE",
        "integer": "NUMBER(38,0)",
        "real": "REAL",
        "smallint": "NUMBER(5,0)",
        "text": "VARCHAR"
        # Add more data type mappings as needed
        # bytea or any large object is not supported
    }

    # Class variables
    _PG_CURSOR = None
    _SF_CURSOR = None
    _PG_DB_NAME = None
    _PG_TABLE_NAME = None
    _SF_DB_NAME = None
    _SF_TABLE_NAME = None
    _PG_SCHEMA_NAME = None
    _SF_SCHEMA_NAME = None
    _CSV_STG_PATH = None
    _LIST_OF_COLUMNS = "*"

    def __init__(self, pg_cursor, sf_cursor, pg_db_name, pg_table_name, sf_db_name, sf_table_name,
                 pg_schema_name="public", sf_schema_name="PUBLIC", csv_stg_path=""):
        # Initialize the object with the provided parameters
        self._PG_CURSOR = pg_cursor
        self._SF_CURSOR = sf_cursor
        self._PG_DB_NAME = pg_db_name
        self._PG_TABLE_NAME = pg_table_name
        self._SF_DB_NAME = sf_db_name
        self._SF_TABLE_NAME = sf_table_name
        self._PG_SCHEMA_NAME = pg_schema_name
        self._SF_SCHEMA_NAME = sf_schema_name

        if (csv_stg_path == ""):
            # If no CSV staging path is provided, generate a default path based on the database, schema, and table names
            self._CSV_STG_PATH = f"dea_{pg_db_name}_{pg_schema_name}_{pg_table_name}_stg.csv".lower()
        else:
            self._CSV_STG_PATH = csv_stg_path

    def getPgTableStructure(self):
        # Fetches the structure of the PostgreSQL table
        # Returns a list of tuples: [(column_name1, data_type1), (column_name2, data_type2), ...]
        # bytea or any large object is not supported

        data_types = ",".join([f"'{data_type}'" for data_type in self.DATA_TYPE_MAPPING.keys()])

        # Query to fetch table structure from information_schema.columns
        query_info_schema_columns = f" \
            SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_scale \
            FROM {self._PG_DB_NAME}.information_schema.columns \
            WHERE table_schema = '{self._PG_SCHEMA_NAME}' AND table_name = '{self._PG_TABLE_NAME}' \
                AND data_type in ({data_types})"""

        # Fetch table metadata from PostgreSQL
        self._PG_CURSOR.execute(query_info_schema_columns)
        columns = self._PG_CURSOR.fetchall()

        table_name = f"{self._PG_DB_NAME}.{self._PG_SCHEMA_NAME}.{self._PG_TABLE_NAME}"

        # Raise error if the source table is not found in information_schema.columns
        if len(columns) == 0:
            raise ValueError(f"Table {table_name} do not exists.")

        self._LIST_OF_COLUMNS = [column_name for column_name, *_ in columns]

        return columns

    def mapPgColumnsDataTypeToSf(self, columns):
        # Maps PostgreSQL data types to Snowflake data types
        # Returns the corresponding Snowflake data type for each column

        table_structure = []

        for column in columns:
            column_name = column[0]
            data_type = column[1]
            length = column[2]

            # Map PostgreSQL data types to Snowflake data types using the data type mapping dictionary
            snowflake_data_type = self.DATA_TYPE_MAPPING.get(data_type, "VARCHAR")  # Default to VARCHAR if no mapping found

            # For character varying data type, use the lambda function to include the length
            if data_type == "character varying":
                snowflake_data_type = snowflake_data_type(length)

            table_structure.append((column_name, snowflake_data_type))

        return table_structure

    def createOrReplaceSfTargetTable(self, table_structure):
        # Creates or replaces the Snowflake target table using the provided table structure
        self._SF_CURSOR.execute(self._generateSfCreateTableScript(table_structure))

    def truncateSfTargetTable(self):
        # Truncates the Snowflake target table
        table_name = f"{self._SF_DB_NAME}.{self._SF_SCHEMA_NAME}.{self._SF_TABLE_NAME}"
        self._SF_CURSOR.execute(f"TRUNCATE TABLE {table_name}")

    def loadPgDataToCsvStg(self, csv_stg_path="", replace_default_stg_path=False):
        # Fetches PostgreSQL data and writes it to a CSV staging file located at the given file path

        if (replace_default_stg_path):
            self._CSV_STG_PATH = csv_stg_path

        csv_stg_path = self._CSV_STG_PATH
        table_name = f"{self._PG_DB_NAME}.{self._PG_SCHEMA_NAME}.{self._PG_TABLE_NAME}"
        self._PG_CURSOR.execute(f"SELECT {','.join(self._LIST_OF_COLUMNS)} FROM {table_name}")
        rows = self._PG_CURSOR.fetchall()

        # Open the CSV file in write mode and write the rows
        with open(csv_stg_path, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            writer.writerow(self._LIST_OF_COLUMNS)  # Write the header row
            writer.writerows(rows)  # Write the data rows

    def loadSfDataFromCsvStg(self, csv_stg_path="", replace_default_stg_path=False):
        # Loads data from the CSV staging file into the existing Snowflake target table

        if (replace_default_stg_path):
            self._CSV_STG_PATH = csv_stg_path

        csv_stg_path = self._CSV_STG_PATH

        db_n_schema = f"{self._SF_DB_NAME}.{self._SF_SCHEMA_NAME}"
        staged_file = f"{db_n_schema}.%{self._SF_TABLE_NAME}"
        self._SF_CURSOR.execute(f"PUT file://{csv_stg_path} @{staged_file} OVERWRITE = TRUE")

        copy_into_script = f"COPY INTO {db_n_schema}.{self._SF_TABLE_NAME} \
            FROM @{db_n_schema}.%{self._SF_TABLE_NAME} \
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"', SKIP_HEADER = 1)"
        self._SF_CURSOR.execute(copy_into_script)

    def setCsvStgPath(self, newPath):
        # Sets the CSV staging file path
        self._CSV_STG_PATH = newPath
        return self._CSV_STG_PATH

    def getCsvStgPath(self):
        # Returns the current CSV staging file path
        return self._CSV_STG_PATH

    def checkIfSfTargetTableExists(self):
        # Checks if the Snowflake target table exists
        table_name = f"{self._SF_DB_NAME}.{self._SF_SCHEMA_NAME}.{self._SF_TABLE_NAME}"
        query = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name.upper()}'"
        self._SF_CURSOR.execute(query)
        exists = self._SF_CURSOR.fetchone() is not None

        return exists

    def autoIngest(self, replace_table=False, truncate_table=True):
        # Automates the process of ingesting data from PostgreSQL to Snowflake

        pg_columns = self.getPgTableStructure()
        sf_columns = self.mapPgColumnsDataTypeToSf(pg_columns)
        self.loadPgDataToCsvStg()

        if (replace_table or not self.checkIfSfTargetTableExists()):
            self.createOrReplaceSfTargetTable(sf_columns)
        elif (truncate_table):
            self.truncateSfTargetTable()

        self.loadSfDataFromCsvStg()
        self._deleteCsvStg()

    def _generateSfCreateTableScript(self, sf_table_structure):
        # Generates the Snowflake SQL script to create or replace the target table
        columns = []
        for column_name, column_type in sf_table_structure:
            columns.append(f"{column_name} {column_type}")

        table_name = f"{self._SF_DB_NAME}.{self._SF_SCHEMA_NAME}.{self._SF_TABLE_NAME}"
        create_table_script = f"CREATE OR REPLACE TABLE {table_name} ({','.join(columns)})"

        return create_table_script

    def _deleteCsvStg(self):
        # Deletes the CSV staging file
        try:
            os.remove(self._CSV_STG_PATH)
            print(f"File '{self._CSV_STG_PATH}' deleted successfully.")
        except FileNotFoundError:
            raise FileNotFoundError(f"File '{self._CSV_STG_PATH}' not found.")
        except PermissionError:
            raise PermissionError(f"Permission denied. Unable to delete file '{self._CSV_STG_PATH}'.")
        except OSError as e:
            raise OSError(f"An error occurred while deleting file '{self._CSV_STG_PATH}': {e}")
    

if __name__ == "__main__":
    # Execute the main function when the module is run directly
    postgres_connection = psycopg2.connect(**PG_CONN_PARAMS)

    pg_cursor = postgres_connection.cursor()

    snowflake_connection = snowflake.connector.connect(**SF_CONN_PARAMS)

    snowflake_cursor = snowflake_connection.cursor()

    PG_DB_NAME = "defaultdb"
    SF_DB_NAME = "NORTHWIND"
    PG_SCHEMA = "public"
    SF_SCHEMA = "PUBLIC"
    CSV_STG = (
        "/home/dea_chintiaputri/products_stg.csv",
        "/home/dea_chintiaputri/orders_stg.csv",
        "/home/dea_chintiaputri/order_details_stg.csv",
        "/home/dea_chintiaputri/categories_stg.csv",
    )

    TableToIngest = (
        (PG_DB_NAME, "products", SF_DB_NAME, "PRODUCTS", PG_SCHEMA, SF_SCHEMA, CSV_STG[0]),
        (PG_DB_NAME, "orders", SF_DB_NAME, "ORDERS", PG_SCHEMA, SF_SCHEMA, CSV_STG[1]),
        (PG_DB_NAME, "order_details", SF_DB_NAME, "ORDER_DETAILS", PG_SCHEMA, SF_SCHEMA, CSV_STG[2]),
        (PG_DB_NAME, "categories", SF_DB_NAME, "CATEGORIES", PG_SCHEMA, SF_SCHEMA, CSV_STG[3])
    )

    products_ingester = PgToSfDataIngester(pg_cursor, snowflake_cursor,*TableToIngest[0])
    orders_ingester = PgToSfDataIngester(pg_cursor, snowflake_cursor,*TableToIngest[1])
    order_details_ingester = PgToSfDataIngester(pg_cursor, snowflake_cursor,*TableToIngest[2])
    categories_ingester = PgToSfDataIngester(pg_cursor, snowflake_cursor,*TableToIngest[3])
    
    products_ingester.autoIngest()
    categories_ingester.autoIngest()
    orders_ingester.autoIngest()
    order_details_ingester.autoIngest()

    pg_cursor.close()
    postgres_connection.close()
    snowflake_cursor.close()
    snowflake_connection.close()