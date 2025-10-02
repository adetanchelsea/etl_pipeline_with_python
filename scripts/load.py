"""
YouTube Data Loading Script

This script loads cleaned YouTube channel data into a Snowflake data warehouse. It ensures that transformed YouTube data is reliably stored in Snowflake
for further analysis and reporting.

"""

# Importing the neccesary libraries
import os
from dotenv import load_dotenv
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging

# Configuring the logging library
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

#-------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#-------------------------------------------------------------------------------------------------
def connect_snowflake(user, password, account, warehouse, database, schema):
    """
    Establish a connection to a Snowflake warehouse.

    Parameters:
    user (str): Snowflake username.
    password (str): Snowflake password.
    account (str): Snowflake account identifier.
    warehouse (str): Target Snowflake warehouse.
    database (str): Target database.
    schema (str): Target schema.

    Returns:
    Active Snowflake connection.
    """
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

def load_dataframe_to_snowflake(conn, df, table_name):
    """
    Load a pandas DataFrame into a Snowflake table using write_pandas.

    Parameters:
    conn: Active Snowflake connection.
    df (pd.DataFrame): DataFrame to be inserted.
    table_name (str): Target table name in Snowflake.

    Returns:
    Number of rows successfully inserted.
    """
    success, nchunks, nrows, _ = write_pandas(conn, df, table_name)
    return nrows

#--------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#--------------------------------------------------------------------------------------------------
def main_loading_script():
    """Main function to execute CSV loading into Snowflake."""

    # CSV path
    csv_file_path = "path_to_cleandata"

    # Snowflake credentials
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    table_name =  os.getenv("SNOWFLAKE_TABLE") 

    # Load CSV
    df = pd.read_csv(csv_file_path)

    df.columns = [col.upper() for col in df.columns]

    # Connect to Snowflake
    with connect_snowflake(user, password, account, warehouse, database, schema) as conn:
        
        rows_inserted = load_dataframe_to_snowflake(conn, df, table_name)

        logging.info(f"Rows inserted: {rows_inserted}")


if __name__ == "__main__":
    main_loading_script()