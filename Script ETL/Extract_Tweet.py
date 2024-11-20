import subprocess
from datetime import datetime, timedelta

def extract_tweet(token, keyword, limit=100):
    """
    Function to extract tweets by cycling through seasonal date ranges from 2022 to 2024.
    The output filename includes the keyword, season, year, and current timestamp for uniqueness.
    """
    # Define seasonal ranges with approximate dates
    seasons = [
        {"name": "Fall", "start_month": 9, "end_month": 11},
        {"name": "Winter", "start_month": 12, "end_month": 2},
        {"name": "Spring", "start_month": 3, "end_month": 5},
        {"name": "Summer", "start_month": 6, "end_month": 8},
    ]
    
    # Define the range of years to cycle through
    start_year = 2023
    end_year = 2024
    
    # Calculate total periods (seasons * years)
    total_periods = (end_year - start_year + 1) * len(seasons)
    
    # Get the current hour and determine the current period
    current_hour = datetime.now().hour
    period_index = current_hour % total_periods  # Cycle through periods
    
    # Determine the season and year for the current period
    year = start_year + (period_index // len(seasons))
    season = seasons[period_index % len(seasons)]
    
    # Calculate `since` and `until` dates for the selected season
    if season["start_month"] <= season["end_month"]:
        # Normal season (e.g., Fall, Spring, Summer)
        since_date = datetime(year, season["start_month"], 1)
        until_date = datetime(year, season["end_month"] + 1, 1) - timedelta(days=1)
    else:
        # Winter season spans across two years
        since_date = datetime(year, season["start_month"], 1)
        until_date = datetime(year + 1, season["end_month"] + 1, 1) - timedelta(days=1)
    
    # Format the dates
    since_date_str = since_date.strftime('%Y-%m-%d')
    until_date_str = until_date.strftime('%Y-%m-%d')
    
    # Create a unique filename
    normalized_keyword = keyword.lower().replace(' ', '_')
   
    filename = f"{normalized_keyword}_{season['name']}_{year}.csv"
    
    # Add the date range to the search query
    search_query = f"{keyword} since:{since_date_str} until:{until_date_str}"
    
    # Build the tweet-harvest command
    command = [
        "npx", "-y", "tweet-harvest@2.6.1",
        "-o", filename,
        "-s", search_query,
        "--tab", "TOP",
        "-l", str(limit),
        "--token", token
    ]
    
    try:
        # Execute the command
        subprocess.run(command, check=True)
        print(f"Data successfully saved to {filename}")
        print(f"Scraped season: {season['name']} {year}")
        print(f"Date range: {since_date_str} to {until_date_str}")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while executing the tweet-harvest command: {e}")

def extract_tweet_all(token, keyword, limit=250):
    """
    Scrape one keyword in a year
    """
    
    # Add the date range to the search query
    search_query = f"{keyword} since:2023-11-22 until:2024-11-20"
    normalized_keyword = keyword.lower().replace(' ', '_')
    filename = f"{normalized_keyword}"

    # Build the tweet-harvest command
    command = [
        "npx", "-y", "tweet-harvest@2.6.1",
        "-o", filename,
        "-s", search_query,
        "--tab", "TOP",
        "-l", str(limit),
        "--token", token
    ]
    
    subprocess.run(command, check=True)
    print(f"Data successfully saved to {filename}")