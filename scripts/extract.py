""""
EXTRACTING THE DATA FROM YOUTUBE

This script extracts candidate YouTube channels that post about "data analysis" using the YouTube Data API. It collects:
- channel ID
- channel title
- description
- subscribers, total views, total video count
- country
- uploads playlist ID (used to fetch all videos)
- appearance frequency in searches (as a relevance measure)
The output of this script is a CSV file with the extracted youtube data.

"""

# Importing the neccesary libraries
import time
from collections import Counter
from typing import List, Dict, Any

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Insert your API Key here
API_KEY = "<PUT_YOUR_KEY_HERE>"

#---------------------------------------------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------------------------------------------
def build_youtube_client(api_key):
    """
    Create a YouTube API client.

    Parameters:
    api_key (str): YouTube Data API key.

    Returns:
    YouTube API client object.
    """
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

