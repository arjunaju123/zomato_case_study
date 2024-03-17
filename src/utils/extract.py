import pandas as pd
from bs4 import BeautifulSoup
from joblib import Parallel, delayed
import multiprocessing
from tqdm import tqdm

def extract_lat_long(url):
    """
    Extract latitude and longitude from a given URL.
    """
    import requests

    header = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Mobile Safari/537.36'
    }
    response = requests.get(url, headers=header)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        script_content = soup.find('script', string=lambda text: text and 'window.__PRELOADED_STATE__' in text)
        if script_content:
            latitude_start_index = script_content.string.find(r'"latitude\":\"') + len(r'"latitude\":\"')
            latitude_end_index = script_content.string.find('",', latitude_start_index)
            latitude = script_content.string[latitude_start_index:latitude_end_index-1]

            longitude_start_index = script_content.string.find(r'"longitude\":\"') + len(r'"longitude\":\"')
            longitude_end_index = script_content.string.find('",', longitude_start_index)
            longitude = script_content.string[longitude_start_index:longitude_end_index-1]

            return latitude, longitude
    return None, None

def fetch_lat_long_parallel(input_csv_path, output_csv_path):
    """
    Fetch latitude and longitude values for rows with missing values in a given dataset.
    Save the updated dataset to a new CSV file.
    """
    # Read the dataset into a DataFrame
    df = pd.read_csv(input_csv_path)

    # Convert 'RATING' column to numeric type
    df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce')  

    # Check if 'Latitude' and 'Longitude' columns exist, if not create them
    if 'Latitude' not in df.columns:
        df['Latitude'] = None

    if 'Longitude' not in df.columns:
        df['Longitude'] = None

    # Filter rows where Latitude or Longitude is None
    filtered_df = df[(df['Latitude'].isnull()) | (df['Longitude'].isnull())]

    # Use multiprocessing to speed up processing
    num_cores = multiprocessing.cpu_count()  # Number of CPU cores

    # Function to extract latitude and longitude from a URL using tqdm
    def extract_lat_long_tqdm(row):
        return extract_lat_long(row['URL'])

    # Apply tqdm to Parallel processing
    results = Parallel(n_jobs=num_cores)(
        delayed(extract_lat_long_tqdm)(row) for index, row in tqdm(filtered_df.iterrows(), total=len(filtered_df), desc="Processing")
    )

    # Assign results to DataFrame
    filtered_df[['Latitude', 'Longitude']] = results

    # Update the original DataFrame with the fetched latitude and longitude values
    df.update(filtered_df)

    # Save the updated DataFrame to a new CSV file
    df.to_csv(output_csv_path, index=False)

# Example usage:
#fetch_lat_long_parallel(r"..\path_to_merged_csv_file", r"..\path_to_scraped_csv_file")

