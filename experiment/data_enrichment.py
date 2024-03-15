#Initially tried with below code as it is too slow in fetching every data and entering into columns. Therefore a 
#parallelism approach is followed in data_enrichment_parallel.py and data_enrichment_parallel2.py files.

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time

# Function to extract latitude and longitude from a URL
def extract_lat_long(url):
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

# Read your dataset into a DataFrame
df = pd.read_csv("..\data\combined_data.csv")

# Add empty columns for latitude and longitude
df['Latitude'] = ''
df['Longitude'] = ''

# Assuming df is your DataFrame
data_rows = len(df)  # Get the number of rows in your DataFrame

# Convert 'RATING' column to numeric type
df['RATING'] = pd.to_numeric(df['RATING'], errors='coerce')  # 'coerce' will convert non-numeric values to NaN
# Filter hotels with rating greater than 4.5
highest_rated_hotels = df[df['RATING'] > 4.5]

# Count the total number of highest rated hotels
total_highest_rated_hotels = len(highest_rated_hotels)
print("Total number of highest rated hotels ", total_highest_rated_hotels)

# Loop through each row in the DataFrame
# Use tqdm as a wrapper around your loop
highest_rated_count = 0  # Counter to track the number of highest rated hotels processed

for index, row in tqdm(df.iterrows(), total=data_rows, desc="Processing"):
    # Check if the rating is greater than 4.5
        url = row['URL']
        # Extract latitude and longitude from the URL
        latitude, longitude = extract_lat_long(url)
        # Update DataFrame with latitude and longitude
        df.at[index, 'Latitude'] = latitude
        df.at[index, 'Longitude'] = longitude

        # highest_rated_count += 1  # Increment counter
        
        # if highest_rated_count == total_highest_rated_hotels:
        #     # All highest rated hotels are processed, break out of the loop
        #     break

# Save the updated DataFrame to a new CSV file
df.to_csv(r"..\data\restaurant_data_with_lat_long_full.csv", index=False)
