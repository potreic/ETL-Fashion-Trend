import os
import json

def transform_pinterest(region, keyword, input_dir={Directory of Input}, output_dir={Directory of Output}):
    """
    Transforms the Pinterest trends data into a simplified time series format.

    Args:
        region (str): Region code (e.g., 'CA').
        keyword (str): Keyword (e.g., 'cardigan').
        input_dir (str): Directory where input JSON files are stored.
        output_dir (str): Directory where transformed JSON files will be saved.

    Returns:
        str: Path of the transformed JSON file.
    """
    # Expand user paths and ensure directories exist
    input_dir = os.path.expanduser(input_dir)
    output_dir = os.path.expanduser(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    input_file = os.path.join(input_dir, f"{keyword}_{region}.json")
    output_file = os.path.join(output_dir, f"transformed_{keyword}_{region}.json")

    try:
        # Read input JSON data
        with open(input_file, 'r') as infile:
            data = json.load(infile)

        # Extract trends and filter for the specified keyword
        time_series_data = {}
        for trend in data.get("trends", []):
            if keyword in trend.get("keyword", "").lower():  # Check if keyword is in the trend's keyword
                time_series_data.update(trend.get("time_series", {}))  # Merge time series data

        # Write the transformed data to the output file
        with open(output_file, 'w') as outfile:
            json.dump(time_series_data, outfile, indent=4)

        print(f"Transformed data saved to {output_file}")
        return output_file
    except FileNotFoundError:
        print(f"Input file not found: {input_file}")
    except json.JSONDecodeError:
        print(f"Invalid JSON format in file: {input_file}")
    except Exception as e:
        print(f"An error occurred: {e}")
