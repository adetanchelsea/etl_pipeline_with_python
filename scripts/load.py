"""
YouTube Data Loading Script

This script loads cleaned YouTube channel data into a Snowflake data warehouse. It ensures that transformed YouTube data is reliably stored in Snowflake
for further analysis and reporting.

"""

# Importing the neccesary libraries
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

#-------------------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#-------------------------------------------------------------------------------------------------
def load_csv(csv_path):
    """
    Load a CSV file into a pandas DataFrame.

    Parameters:
    csv_path (str): Path to the CSV file.

    Returns:
    DataFrame containing CSV data.
    """

    return pd.read_csv(csv_path)

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
    csv_file_path = "data/cleaned_youtube_data.csv"

    # Snowflake credentials
    user = 'USERNAME'
    password = 'PASSWORD'
    account = 'ACCOUNT_NAME'
    warehouse = 'MY_WAREHOUSE'
    database = 'MY_DB'
    schema = 'MY_SCHEMA'
    table_name = 'MY_TABLE'

    # Load CSV
    df = load_csv(csv_file_path)

    df.columns = [col.upper() for col in df.columns]

    # Connect to Snowflake
    conn = connect_snowflake(user, password, account, warehouse, database, schema)

    # Load data into Snowflake
    rows_inserted = load_dataframe_to_snowflake(conn, df, table_name)
    print(f"Rows inserted: {rows_inserted}")

    # Close connection
    conn.close()


if __name__ == "__main__":
    main_loading_script()