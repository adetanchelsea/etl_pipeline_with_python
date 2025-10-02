"""
YouTube Data Transformation Script

This script transforms raw extracted YouTube channel data into a clean, analysis-ready format by performing the following steps:

"""
# Importing the neccesary libraries
import pandas as pd
import re
import pycountry
import logging

# Configuring the logging library
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)

#-----------------------------------------------------------------------------------------------------------------
#HELPER FUNCTIONS
#-----------------------------------------------------------------------------------------------------------------
def standardize_column_names(df):
    """
    Standardize column names by making them lowercase, stripping extra spaces, and replacing spaces with underscores.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with standardized column names.
    """

    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df


def drop_duplicates(df):
    """
    Remove duplicate rows from the DataFrame.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with duplicates removed.
    """

    return df.drop_duplicates()

def handle_missing_values(df):
    """
    Handle missing values in selected columns by filling defaults.
    
    - 'country' → 'Unknown'
    - 'description' → 'Unknown'
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with missing values handled.
    """

    df["country"] = df["country"].fillna("Unknown")
    df["description"] = df["description"].fillna("Unknown")
    return df

def convert_data_types(df):
    """
    Convert selected columns to numeric and datetime types.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with corrected data types.
    """
    numeric_cols = ["subscriber_count", "view_count", "video_count", "appearances_in_searches"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")
    return df


def map_country_names(df):
    """
    Map country codes/aliases to full country names using pycountry. If mapping fails, keep the original value.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with 'country' column standardized to full names.
    """
    def get_country_name(code):
        if pd.isna(code) or code == "Unknown":
            return "Unknown"
        try:
            country = pycountry.countries.lookup(code.strip())
            return country.name
        except LookupError:
            return code 
    df["country"] = df["country"].apply(get_country_name)
    return df

def clean_text_fields(df):
    """
    Clean text fields by removing special characters and trimming whitespace.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with cleaned text fields.
    """
    def clean_text(text):
        if pd.isna(text):
            return ""
        text = re.sub(r"[^a-zA-Z0-9\s.,!?]", "", str(text))
        return text.strip()
    
    df["description"] = df["description"].apply(clean_text)
    df["title"] = df["title"].str.strip()
    return df

def rename_columns(df):
    """
    Rename columns for clarity.
    
    Parameters:
    df (pd.DataFrame): Input DataFrame.
    
    Returns:
    DataFrame with renamed columns.
    """
    return df.rename(columns={"uploads_playlist_id": "playlist_id"})

#--------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#--------------------------------------------------------------------------------------------------
def transform_youtube_data(input_path, output_path):
    """
    Run the full YouTube data transformation pipeline.
    
    Parameters:
    input_path (str): Path to raw CSV file.
    output_path (str): Path to save cleaned CSV file.
    
    Returns:
    None
    """

    df = pd.read_csv(input_path)
    df = standardize_column_names(df)
    df = drop_duplicates(df)
    df = handle_missing_values(df)
    df = convert_data_types(df)
    df = map_country_names(df)
    df = clean_text_fields(df)
    df = rename_columns(df)
    df.to_csv(output_path)

if __name__ == "__main__":
    input_file = "data/extracted_youtube_data"
    output_file = "data/cleaned_youtube_data"
    
    transform_youtube_data(input_file, output_file)
    logging.info(f"Data transformed successfully! Saved to {output_file}")







