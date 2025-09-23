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
API_KEY = "YOUR API_KEY HERE"

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
    return build("youtube", "v3", developerKey=api_key)

def safe_int(x):
    """
    Convert a string or number to int safely.

    Parameters:
    x (Any): Input value.

    Returns:
    Integer value if conversion succeeds, otherwise None.
    """
    try:
        return int(x)
    except Exception:
        return None
    
def paginated_search_channels(youtube, query, max_channels: int = 200, search_type: str = "channel"):
    """
    Search YouTube for channels or videos matching a query and return channel IDs.

    Parameters:
    youtube(client): YouTube API client.
    query(str): Search keyword ("data analysis tutorial").
    max_channels(int): Maximum number of unique channels to fetch.
    search_type(str): "channel" to search channels directly, "video" to search videos and extract their channel IDs.

    Returns:
    List of channel IDs.
    """
    found = []
    next_page = None
    per_page = 50 
    while True:
        try:
            req = youtube.search().list(
                part="snippet",
                q=query,
                type=search_type,
                maxResults=per_page,
                pageToken=next_page
            )
            res = req.execute()
        except HttpError as e:
            print("HttpError during search:", e)
            time.sleep(5)
            break

        for item in res.get("items", []):
            ch_id = item.get("snippet", {}).get("channelId") or item.get("id", {}).get("channelId")
            if ch_id:
                found.append(ch_id)

        next_page = res.get("nextPageToken")
        if len(set(found)) >= max_channels or not next_page:
            break

    return found

def gather_candidate_channels(youtube, queries, max_channels_per_query=100) -> Counter:
    """
    Run searches for multiple queries and return channel frequency counts.

    Parameters:
    youtube(client): YouTube API client.
    queries(list): List of search keywords.
    max_channels_per_query (int): Max channels to fetch per query type.

    Returns:
    Channel IDs mapped to how many times they appeared.
    """
    c = Counter()
    for q in queries:
        channel_hits = paginated_search_channels(youtube, q, max_channels=max_channels_per_query, search_type="channel")
        c.update(channel_hits)

        video_hits = paginated_search_channels(youtube, q, max_channels=max_channels_per_query, search_type="video")
        c.update(video_hits)

    return c

def get_channel_stats(youtube, channel_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch metadata and statistics for a batch of channels.

    Parameters:
    youtube (client): YouTube API client.
    channel_ids (list): List of channel IDs.

    Returns:
    List of channel records with snippet, statistics, and content details.
    """
    results = []
    BATCH = 50
    for i in range(0, len(channel_ids), BATCH):
        batch = channel_ids[i:i + BATCH]
        ids_str = ",".join(batch)
        try:
            resp = youtube.channels().list(
                part="snippet,statistics,contentDetails",
                id=ids_str,
                maxResults=50
            ).execute()
        except HttpError as e:
            print("HttpError fetching channel stats:", e)
            time.sleep(5)
            continue

        for ch in resp.get("items", []):
            snippet = ch.get("snippet", {})
            stats = ch.get("statistics", {})
            details = ch.get("contentDetails", {})
            results.append({
                "channel_id": ch.get("id"),
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "published_at": snippet.get("publishedAt"),
                "country": snippet.get("country"),
                "subscriber_count": safe_int(stats.get("subscriberCount")),
                "view_count": safe_int(stats.get("viewCount")),
                "video_count": safe_int(stats.get("videoCount")),
                "uploads_playlist_id": details.get("relatedPlaylists", {}).get("uploads")
            })
    return results

#-------------------------------------------------------------------------------------------------
# MAIN FUNCTION
#-------------------------------------------------------------------------------------------------
def main_extract(api_key=API_KEY, target_channels=200, output_csv="extracted_youtube_data.csv"):
    """
    Main extraction pipeline.

    Steps:
    - Search for channels/videos matching data analysis keywords.
    - Collect channel IDs and count their frequency.
    - Fetch metadata and stats for top unique channels.
    - Save results to CSV.

    Parameters:
    api_key (str): YouTube Data API key.
    target_channels (int): Number of unique channels to include.
    output_csv (str): Output CSV file.

    Returns:
    Pandas DataFrame containing channel information.
    """
    youtube = build_youtube_client(api_key)

    queries = [
        "data analysis tutorial",
        "python pandas tutorial",
        "excel for data analysis",
        "sql for data analysis",
        "power bi tutorial",
        "tableau tutorial",
        "data analyst tutorial",
        "data analysis project",
        "learn data analysis",
    ]

    print("Collecting candidate channels by running searches...")
    channel_counter = gather_candidate_channels(youtube, queries, max_channels_per_query=200)

    unique_channels = [ch for ch, _ in channel_counter.most_common(target_channels)]
    print(f"Found {len(unique_channels)} unique channel candidates; pulling stats...")

    channel_records = get_channel_stats(youtube, unique_channels)

    for rec in channel_records:
        rec["appearances_in_searches"] = channel_counter.get(rec["channel_id"], 0)

    df = pd.DataFrame(channel_records)

    cols = [
        "channel_id", "title", "appearances_in_searches",
        "subscriber_count", "view_count", "video_count",
        "published_at", "country", "uploads_playlist_id", "description"
    ]
    df = df.reindex(columns=[c for c in cols if c in df.columns])

    df.to_csv(output_csv, index=False)
    print(f"Saved {len(df)} channels to {output_csv}")
    return df

if __name__ == "__main__":
    df = main_extract()
    print(df.head())





