# Building a Youtube Data ETL Pipeline with Python

This repository houses all the scripts used in extracting data from youtube, transforming it and then loading it into a Snowflake Data Warehouse. 

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Setup](#project-setup)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Environment Variables](#environment-variables)
- [How It Works](#how-it-works)
- [Usage](#usage)
- [Future Enhancements](#future-enhancements)
- [Useful Resources](#useful-resources)

---

## Overview
This project implements an automated ETL (Extract, Transform, Load) pipeline for collecting and analyzing YouTube channel data related to “data analysis.” The pipeline extracts channel and video metadata using the YouTube Data API, cleans and transforms the data, such as standardizing country names and removing special characters from descriptions, and loads it into a Snowflake database for storage and further analysis.

## Features
- Extracts YouTube channel and video data using the YouTube Data API.
- Cleans and transforms data, standardizing country names and removing special characters.
- Loads data into a Snowflake database for storage and analysis.
- Modular and configurable pipeline using environment variables for easy replication.

## Architecture
![Architecture Diagram](/img/etlarchitecture.svg)

## Technologies Used
- **Python** – Main programming language for the ETL pipeline
- **pandas** – Data manipulation and cleaning
- **requests** – API calls to YouTube Data API
- **pycountry** – Standardizing country names
- **google-api-python-client** – Interfacing with YouTube API
- **snowflake-connector-python** – Connecting and loading data into Snowflake

## Project Setup
### Prerequisites
Before running the project, make sure you have the following:
- Python 3.10+ installed on your system.
-A YouTube Data API key to access channel and video data:
    - Go to the Google Cloud Console.
    - Create a new project or select an existing one.
    - Navigate to APIs & Services, Library and enable the YouTube Data API v3.
    - Go to APIs & Services, Credentials and create an API key.
- Snowflake account credentials (username, password, account, warehouse, database, schema).
- pip to install Python dependencies from requirements.txt.
- A code editor like VS Code or Sublime Text to write and run your Python scripts.
- Basic familiarity with running Python scripts in your terminal or IDE.

### Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/adetanchelsea/etl_pipeline_with_python.git
   cd etl_pipeline_with_python
   ```
2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```
### Environment Variables
Create an `.env` file and add the following:

    ```bash
    
    # YouTube API key
    YOUTUBE_API_KEY=your_youtube_api_key_here

    # Snowflake credentials
    SNOWFLAKE_USER=your_username
    SNOWFLAKE_PASSWORD=your_password
    SNOWFLAKE_ACCOUNT=your_account
    SNOWFLAKE_WAREHOUSE=your_warehouse
    SNOWFLAKE_DATABASE=your_database
    SNOWFLAKE_SCHEMA=your_schema

    #QUERY SEARCH PHRASES
    QUERY="query_list"

    ```

## How It Works
- Data Extraction: The pipeline uses the YouTube Data API to fetch channel and video metadata, including subscribers, views, video count, and uploads playlist ID.

- Data Transformation: The extracted data is cleaned and standardized.

- Data Loading: The cleaned data is loaded into a Snowflake database, allowing for secure storage and easy querying.

## Usage
- Activate your virtual environment (if not already active).
- Ensure your .env file contains your YouTube API key, Snowflake credentials and query search phrases.
- Run the ETL scripts individually or in sequence:

    ```bash
    # Extract data from YouTube
    scripts/extract.py

    # Transform the extracted data
    scripts/transform.py

    # Load transformed data into Snowflake
    scripts/load.py
    
    ```
- Verify the Snowflake database to confirm that the data has been loaded correctly.

## Future Enhancements
- Extend the pipeline to analyze the collected data and identify the top-performing YouTube channels focused on “data analysis.”

- Integrate visualization tools to create dashboards and insights from the extracted data.

- Increase the scope of the pipeline to include more channels, videos, or different topics beyond “data analysis.”

- Implement automation to run the ETL pipeline on a regular schedule for continuous data updates.

## Useful Resources
- [Youtube Video on Scraping Data from Youtube](https://www.youtube.com/watch?v=SwSbnmqk3zY)
- [Youtube Data Extraction Repository by Nezzar](https://github.com/N3zzar/Youtube_data_extraction_project)
- [Youtube API Documentation](https://developers.google.com/youtube/v3)


