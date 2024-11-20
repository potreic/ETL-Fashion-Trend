import os
import json
import requests

def extract_pinterest(regions, keywords, access_token):
    """
    Extract Pinterest trending keywords for the specified regions and keywords.
    
    Args:
        regions (list): List of region codes (e.g., ['US', 'CA']).
        keywords (tuple): Tuple of keywords to include in the query (e.g., ('fashion', 'tech')).
        access_token (str): Pinterest API access token.

    Returns:
        dict: A dictionary containing the response data for each region.
    """
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # Define the directory to save JSON files
    save_dir = os.path.expanduser("/home/rekdat/pinterest-data/raw/")
    os.makedirs(save_dir, exist_ok=True)  # Create the directory if it doesn't exist

    collected_data = {}

    for region in regions:
        params = {
            'include_keywords': keywords,  # Include the tuple of keywords
        }

        url = f'https://api.pinterest.com/v5/trends/keywords/{region}/top/growing?trend_type=growing'
        
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            try:
                # Parse the JSON response
                data = response.json()
                collected_data[region] = data
                
                # Construct the file path
                file_name = f"{'_'.join(keywords)}_{region}.json"
                file_path = os.path.join(save_dir, file_name)
                
                # Save the data to the JSON file
                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                
                print(f"Data for region {region} successfully saved to {file_path}")
            except ValueError:
                print(f"Response for region {region} is not in JSON format:", response.text)
        else:
            print(f"Error for region {region}: {response.status_code} - {response.text}")

    return collected_data
