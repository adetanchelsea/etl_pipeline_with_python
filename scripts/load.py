"""
YouTube Data Loading Script

This script loads cleaned YouTube channel data into a Snowflake data warehouse. It ensures that transformed YouTube data is reliably stored in Snowflake
for further analysis and reporting.

"""

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
